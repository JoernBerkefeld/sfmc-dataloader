import assert from 'node:assert/strict';
import { describe, it, before } from 'node:test';
import fs from 'node:fs/promises';
import { createFixtureRoot } from './fixture-root.js';
import path from 'node:path';
import { RestError } from 'sfmc-sdk/util';
import { log } from '../lib/log.mjs';
import { DEFAULT_MAX_BODY_BYTES } from '../lib/batch.mjs';
import {
    importRowsForDe,
    importRowsStreamingForDe,
    importFromFile,
    warnIfImportCountUnexpected,
} from '../lib/import-de.mjs';

/**
 * Captures `console.warn` lines while `fn` runs (used to observe `log.warn` output).
 *
 * @param {() => void} fn
 * @returns {string[]}
 */
/* eslint-disable no-console -- test spy replaces console.warn */
function captureConsoleWarn(function_) {
    const lines = [];
    const orig = console.warn;
    console.warn = (first) => {
        lines.push(String(first));
    };
    try {
        function_();
    } finally {
        console.warn = orig;
    }
    return lines;
}
/* eslint-enable no-console */

async function* failingRowSource() {
    yield { id: '1' };
    throw new Error('synthetic read failure');
}

describe('importRowsForDe', () => {
    it('calls put for upsert in chunks', async () => {
        const calls = [];
        const sdk = {
            rest: {
                put: async (p, body) => {
                    calls.push({ path: p, n: body.items.length });
                    return {};
                },
                post: async () => {
                    throw new Error('unexpected post');
                },
            },
        };
        const rows = Array.from({ length: 3 }, (_, index) => ({ id: String(index) }));
        const result = await importRowsForDe(sdk, {
            deKey: 'K',
            rows,
            mode: 'upsert',
        });
        assert.equal(result.count, 3);
        assert.deepEqual(result.requestIds, [null]);
        assert.equal(calls.length, 1);
        assert.equal(calls[0].n, 3);
        assert.ok(calls[0].path.includes('async'));
    });

    it('throws readable message when PUT returns 400 with resultMessages', async () => {
        const sdk = {
            rest: {
                put: async () => {
                    throw new RestError({
                        response: {
                            status: 400,
                            headers: {},
                            data: {
                                resultMessages: [
                                    { message: 'Primary key field required for upsert operations' },
                                ],
                            },
                        },
                    });
                },
                post: async () => {
                    throw new Error('unexpected');
                },
            },
        };
        await assert.rejects(
            () => importRowsForDe(sdk, { deKey: 'MyDE', rows: [{ a: 1 }], mode: 'upsert' }),
            /Primary key field required for upsert/,
        );
    });

    it('calls post for insert', async () => {
        const sdk = {
            rest: {
                put: async () => {
                    throw new Error('unexpected put');
                },
                post: async () => ({ requestId: 'req-123' }),
            },
        };
        const result = await importRowsForDe(sdk, {
            deKey: 'K',
            rows: [{ a: 1 }],
            mode: 'insert',
        });
        assert.equal(result.count, 1);
        assert.deepEqual(result.requestIds, ['req-123']);
    });
});

describe('importRowsStreamingForDe', () => {
    for (const delta of [0, -1]) {
        it(`honors exact UTF-8 envelope boundaries (budget delta ${delta})`, async () => {
            const rows = [{ value: String.raw`😀é\"` }, { value: '中' }, { value: 'last' }];
            const budget = Buffer.byteLength(JSON.stringify({ items: rows.slice(0, 2) })) + delta;
            const submitted = [];
            const messages = [];
            const originalInfo = log.info;
            log.info = (message) => {
                messages.push(message);
            };
            try {
                const result = await importRowsStreamingForDe(
                    {
                        rest: {
                            put: async (_route, body) => {
                                submitted.push(body.items);
                                return {};
                            },
                        },
                    },
                    {
                        deKey: 'K',
                        mode: 'upsert',
                        rowSource: rows,
                        maxBytesPerBatch: budget,
                        totalMemoryBatches: 1,
                    },
                );
                assert.equal(result.count, rows.length);
                assert.deepEqual(submitted.flat(), rows);
                assert.equal(submitted[0].length, delta === 0 ? 2 : 1);
                assert.ok(
                    submitted.every(
                        (items) => Buffer.byteLength(JSON.stringify({ items })) <= budget,
                    ),
                );
                assert.deepEqual(
                    messages,
                    submitted.map((_, index) => `Uploading batch ${index + 1}`),
                );
            } finally {
                log.info = originalInfo;
            }
        });
    }

    it('pauses wide-row consumption while each sequential SDK request is deferred', async () => {
        let consumed = 0;
        let isClosed = false;
        async function* source() {
            try {
                for (let id = 0; id < 8; id++) {
                    consumed++;
                    yield { id, value: 'x'.repeat(2_000_000) };
                }
            } finally {
                isClosed = true;
            }
        }
        const calls = [];
        const gates = [];
        let nextStarted = Promise.withResolvers();
        const put = async (_route, body) => {
            calls.push(body.items.map((row) => row.id));
            assert.ok(Buffer.byteLength(JSON.stringify(body)) <= DEFAULT_MAX_BODY_BYTES);
            const gate = Promise.withResolvers();
            gates.push(gate);
            nextStarted.resolve();
            return gate.promise;
        };
        const result = importRowsStreamingForDe(
            { rest: { put } },
            {
                deKey: 'K',
                mode: 'upsert',
                rowSource: source(),
                totalMemoryBatches: 1,
            },
        );
        await nextStarted.promise;
        assert.equal(consumed, 3, 'two buffered rows and one lookahead, not 2,500 rows');
        assert.equal(calls.length, 1);
        nextStarted = Promise.withResolvers();
        gates[0].resolve({ requestId: 'accepted-wide' });
        await nextStarted.promise;
        assert.equal(consumed, 5);
        assert.deepEqual(calls, [
            [0, 1],
            [2, 3],
        ]);
        gates[1].reject(new Error('wide submission failure'));
        await assert.rejects(result, (error) => {
            assert.deepEqual(error.importSubmission, {
                acceptedRows: 2,
                acceptedRequests: 1,
                requestIds: ['accepted-wide'],
            });
            return true;
        });
        assert.equal(consumed, 5);
        assert.equal(isClosed, true);
    });

    it('keeps oversized rows alone and preserves order across byte and row flushes', async () => {
        const rows = [
            { id: 0 },
            { id: 1, value: 'x'.repeat(100) },
            { id: 2 },
            { id: 3 },
            { id: 4 },
        ];
        const submitted = [];
        const result = await importRowsStreamingForDe(
            {
                rest: {
                    post: async (_route, body) => {
                        submitted.push(body.items);
                        return { requestId: String(submitted.length) };
                    },
                },
            },
            {
                deKey: 'K',
                mode: 'insert',
                rowSource: rows,
                maxRowsPerBatch: 2,
                maxBytesPerBatch: 40,
            },
        );
        assert.deepEqual(
            submitted.map((items) => items.map((row) => row.id)),
            [[0], [1], [2, 3], [4]],
        );
        assert.deepEqual(submitted.flat(), rows);
        assert.equal(result.count, 5);
        assert.deepEqual(result.requestIds, ['1', '2', '3', '4']);
    });

    it('posts insert payloads from an async row source', async () => {
        const posts = [];
        const sdk = {
            rest: {
                put: async () => {
                    throw new Error('unexpected put');
                },
                post: async (p, body) => {
                    posts.push({ path: p, n: body.items.length });
                    return { requestId: 'r1' };
                },
            },
        };
        // eslint-disable-next-line unicorn/consistent-function-scoping
        async function* source() {
            yield { a: '1' };
            yield { a: '2' };
        }
        const result = await importRowsStreamingForDe(sdk, {
            deKey: 'K',
            rowSource: source(),
            mode: 'insert',
            totalMemoryBatches: 1,
        });
        assert.equal(result.count, 2);
        assert.deepEqual(result.requestIds, ['r1']);
        assert.equal(posts.length, 1);
        assert.equal(posts[0].n, 2);
    });

    for (const mode of ['insert', 'upsert']) {
        it(`reports accepted requests when the second batch rejects (${mode})`, async () => {
            let calls = 0;
            const submit = async () => {
                if (++calls === 2) {
                    throw new Error('synthetic second-batch rejection');
                }
                return { requestId: 'accepted-1' };
            };
            const sdk = { rest: { put: submit, post: submit } };
            await assert.rejects(
                () =>
                    importRowsStreamingForDe(sdk, {
                        deKey: 'K',
                        rowSource: [{ id: '1' }, { id: '2' }, { id: '3' }],
                        mode,
                        maxRowsPerBatch: 2,
                        totalMemoryBatches: 2,
                    }),
                (error) => {
                    assert.match(error.message, /2 rows in 1 requests accepted/);
                    assert.match(error.message, /acceptance does not confirm completion/);
                    assert.match(error.message, /No automatic retry or rollback/);
                    assert.deepEqual(error.importSubmission, {
                        acceptedRows: 2,
                        acceptedRequests: 1,
                        requestIds: ['accepted-1'],
                    });
                    assert.match(error.cause.message, /second-batch rejection/);
                    return true;
                },
            );
            assert.equal(calls, 2);
        });
    }

    it('retains accepted evidence when later source reading fails without a requestId', async () => {
        const sdk = { rest: { put: async () => ({}) } };
        await assert.rejects(
            () =>
                importRowsStreamingForDe(sdk, {
                    deKey: 'K',
                    rowSource: failingRowSource(),
                    mode: 'upsert',
                    maxRowsPerBatch: 1,
                    totalMemoryBatches: 2,
                }),
            (error) => {
                assert.match(error.message, /synthetic read failure/);
                assert.deepEqual(error.importSubmission, {
                    acceptedRows: 1,
                    acceptedRequests: 1,
                    requestIds: [null],
                });
                return true;
            },
        );
    });

    it('throws when the row source yields nothing', async () => {
        const sdk = {
            rest: {
                put: async () => {
                    throw new Error('unexpected');
                },
                post: async () => {
                    throw new Error('unexpected');
                },
            },
        };
        // eslint-disable-next-line unicorn/consistent-function-scoping
        async function* empty() {}
        await assert.rejects(
            () =>
                importRowsStreamingForDe(sdk, {
                    deKey: 'K',
                    rowSource: empty(),
                    mode: 'insert',
                    totalMemoryBatches: 1,
                }),
            /no data rows/,
        );
    });

    it('flushes multiple memory batches when maxRowsPerBatch is small', async () => {
        const posts = [];
        const sdk = {
            rest: {
                put: async () => {
                    throw new Error('unexpected put');
                },
                post: async (_p, body) => {
                    posts.push(body.items.length);
                    return { requestId: 'rx' };
                },
            },
        };
        // eslint-disable-next-line unicorn/consistent-function-scoping
        async function* source() {
            for (let index = 0; index < 5; index++) {
                yield { i: String(index) };
            }
        }
        const result = await importRowsStreamingForDe(sdk, {
            deKey: 'K',
            rowSource: source(),
            mode: 'insert',
            maxRowsPerBatch: 2,
            totalMemoryBatches: 3,
        });
        assert.equal(result.count, 5);
        assert.equal(posts.length, 3);
        assert.deepEqual(posts, [2, 2, 1]);
    });
});

describe('importFromFile', () => {
    for (const format of ['csv', 'tsv', 'json']) {
        for (const skipEmptyLines of [undefined, false, true, 'true']) {
            it(`uses strict opt-in policy and exact values (${format}, ${skipEmptyLines})`, async () => {
                const root = await createFixtureRoot('mcdata-file-policy-');
                const filePath = path.join(root, `source.${format}`);
                const delimiter = format === 'tsv' ? '\t' : ',';
                const rows = [
                    { id: '', value: '' },
                    { id: '1', value: ` snow${delimiter}\nline ` },
                ];
                await fs.writeFile(
                    filePath,
                    format === 'json'
                        ? JSON.stringify(rows)
                        : `\u{FEFF}id${delimiter}value\n   \n""${delimiter}""\n1${delimiter}" snow${delimiter}\nline "\n\n`,
                );
                const submitted = [];
                const sdk = {
                    rest: {
                        put: async (_route, body) => {
                            submitted.push(...body.items);
                            return {};
                        },
                    },
                };
                const operation = importFromFile(sdk, {
                    filePath,
                    deKey: 'K',
                    mode: 'upsert',
                    skipEmptyLines,
                });
                if (format === 'json' || skipEmptyLines === true) {
                    assert.equal((await operation).count, 2);
                    assert.deepEqual(submitted, rows);
                } else {
                    await assert.rejects(operation, /Row length|columns|fields/);
                    assert.deepEqual(submitted, []);
                }
            });
        }
    }

    let temporary;
    before(async () => {
        temporary = await createFixtureRoot('mcdata-import-');
    });
    const stubSdk = {
        rest: {
            put: async () => {
                throw new Error('should not call API when file has no rows');
            },
            post: async () => {
                throw new Error('should not call API when file has no rows');
            },
        },
    };

    for (const format of ['csv', 'tsv']) {
        it(`submits exact 87-column original inline-quote reproduction (${format})`, async () => {
            const separator = format === 'csv' ? ',' : '\t';
            const headers = Array.from({ length: 87 }, (_, index) => `field${index}`);
            const values = headers.map((_, index) =>
                index === 9 ? 'var label = "demo";' : `value-${index}`,
            );
            const next = headers.map((_, index) => `next-${index}`);
            const filePath = path.join(temporary, `inline.${format}`);
            await fs.writeFile(
                filePath,
                [headers, values, next].map((row) => row.join(separator)).join('\n') + '\n',
            );
            const submitted = [];
            const sdk = {
                rest: {
                    put: async (_route, body) => {
                        submitted.push(...body.items);
                        return {};
                    },
                },
            };
            const result = await importFromFile(sdk, { filePath, deKey: 'K', mode: 'upsert' });
            assert.equal(result.count, 2);
            assert.deepEqual(submitted, [
                Object.fromEntries(headers.map((header, index) => [header, values[index]])),
                Object.fromEntries(headers.map((header, index) => [header, next[index]])),
            ]);
        });
    }

    for (const contents of ['   \n\n', 'id,value\n   \n\n']) {
        it(`rejects opt-in skipped-only file without upload (${JSON.stringify(contents)})`, async () => {
            const filePath = path.join(temporary, 'skipped-only.csv');
            await fs.writeFile(filePath, contents);
            await assert.rejects(
                importFromFile(stubSdk, {
                    filePath,
                    deKey: 'K',
                    mode: 'upsert',
                    skipEmptyLines: true,
                }),
                /no data rows/,
            );
        });
    }

    it('rejects malformed data beyond the first upload window without submitting any batch', async () => {
        const filePath = path.join(temporary, 'late.csv');
        await fs.writeFile(filePath, 'id,value\n' + '1,ok\n'.repeat(3000) + '2,bad,extra\n');
        let submissions = 0;
        const sdk = {
            rest: {
                put: async () => {
                    submissions++;
                    return {};
                },
            },
        };
        await assert.rejects(
            () => importFromFile(sdk, { filePath, deKey: 'K', mode: 'upsert' }),
            /Row length/,
        );
        assert.equal(submissions, 0);
    });

    it('rejects empty CSV', async () => {
        const p = path.join(temporary, 'empty.csv');
        await fs.writeFile(p, '', 'utf8');
        await assert.rejects(
            () =>
                importFromFile(stubSdk, {
                    filePath: p,
                    deKey: 'K',
                    mode: 'upsert',
                }),
            /no data rows/,
        );
    });

    it('rejects BOM-only CSV', async () => {
        const p = path.join(temporary, 'bom.csv');
        await fs.writeFile(p, '\u{FEFF}', 'utf8');
        await assert.rejects(
            () =>
                importFromFile(stubSdk, {
                    filePath: p,
                    deKey: 'K',
                    mode: 'upsert',
                }),
            /no data rows/,
        );
    });

    it('rejects header-only CSV', async () => {
        const p = path.join(temporary, 'header.csv');
        await fs.writeFile(p, 'a,b,c\n', 'utf8');
        await assert.rejects(
            () =>
                importFromFile(stubSdk, {
                    filePath: p,
                    deKey: 'K',
                    mode: 'upsert',
                }),
            /no data rows/,
        );
    });

    it('rejects header-only TSV', async () => {
        const p = path.join(temporary, 'header.tsv');
        await fs.writeFile(p, 'a\tb\tc\n', 'utf8');
        await assert.rejects(
            () =>
                importFromFile(stubSdk, {
                    filePath: p,
                    deKey: 'K',
                    mode: 'upsert',
                }),
            /no data rows/,
        );
    });

    it('rejects empty JSON array', async () => {
        const p = path.join(temporary, 'empty.json');
        await fs.writeFile(p, '[]', 'utf8');
        await assert.rejects(
            () =>
                importFromFile(stubSdk, {
                    filePath: p,
                    deKey: 'K',
                    mode: 'upsert',
                }),
            /no data rows/,
        );
    });
});

describe('warnIfImportCountUnexpected', () => {
    const label = 'DE "X"';

    it('insert mode without clear: expects countBefore + imported', () => {
        const lines = captureConsoleWarn(() =>
            warnIfImportCountUnexpected({
                countBefore: 5,
                cleared: false,
                countAfter: 7,
                imported: 2,
                mode: 'insert',
                label,
            }),
        );
        assert.equal(lines.length, 0);
    });

    it('insert mode with cleared DE: expects imported only (not countBefore + imported)', () => {
        const lines = captureConsoleWarn(() =>
            warnIfImportCountUnexpected({
                countBefore: 11,
                cleared: true,
                countAfter: 11,
                imported: 11,
                mode: 'insert',
                label,
            }),
        );
        assert.equal(lines.length, 0);
    });

    it('insert mode without clear warns when countAfter is below expected', () => {
        const lines = captureConsoleWarn(() =>
            warnIfImportCountUnexpected({
                countBefore: 11,
                cleared: false,
                countAfter: 11,
                imported: 11,
                mode: 'insert',
                label,
            }),
        );
        assert.equal(lines.length, 1);
        assert.ok(lines[0].includes('looks unexpected'));
        assert.ok(lines[0].includes('expected at least 22'));
    });

    it('upsert mode with non-empty DE: expects at least imported rows', () => {
        const lines = captureConsoleWarn(() =>
            warnIfImportCountUnexpected({
                countBefore: 100,
                cleared: false,
                countAfter: 50,
                imported: 50,
                mode: 'upsert',
                label,
            }),
        );
        assert.equal(lines.length, 0);
    });

    it('upsert mode with non-empty DE warns when countAfter < imported', () => {
        const lines = captureConsoleWarn(() =>
            warnIfImportCountUnexpected({
                countBefore: 100,
                cleared: false,
                countAfter: 10,
                imported: 50,
                mode: 'upsert',
                label,
            }),
        );
        assert.equal(lines.length, 1);
        assert.ok(lines[0].includes('looks unexpected'));
        assert.ok(lines[0].includes('expected at least 50'));
    });

    it('warns when countAfter is null', () => {
        const lines = captureConsoleWarn(() =>
            warnIfImportCountUnexpected({
                countBefore: 5,
                cleared: false,
                countAfter: null,
                imported: 2,
                mode: 'insert',
                label,
            }),
        );
        assert.equal(lines.length, 1);
        assert.ok(lines[0].includes('Could not verify import result'));
    });

    it('does not warn unexpected when countBefore is null', () => {
        const lines = captureConsoleWarn(() =>
            warnIfImportCountUnexpected({
                countBefore: null,
                cleared: false,
                countAfter: 10,
                imported: 10,
                mode: 'insert',
                label,
            }),
        );
        assert.equal(lines.length, 0);
    });
});
