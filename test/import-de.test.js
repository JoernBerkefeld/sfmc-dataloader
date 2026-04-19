import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { RestError } from 'sfmc-sdk/util';
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
function captureConsoleWarn(fn) {
    const lines = [];
    const orig = console.warn;
    console.warn = (first) => {
        lines.push(String(first));
    };
    try {
        fn();
    } finally {
        console.warn = orig;
    }
    return lines;
}
/* eslint-enable no-console */

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
        const rows = Array.from({ length: 3 }, (_, i) => ({ id: String(i) }));
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
        async function* src() {
            yield { a: '1' };
            yield { a: '2' };
        }
        const result = await importRowsStreamingForDe(sdk, {
            deKey: 'K',
            rowSource: src(),
            mode: 'insert',
            totalMemoryBatches: 1,
        });
        assert.equal(result.count, 2);
        assert.deepEqual(result.requestIds, ['r1']);
        assert.equal(posts.length, 1);
        assert.equal(posts[0].n, 2);
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
        async function* src() {
            for (let i = 0; i < 5; i++) {
                yield { i: String(i) };
            }
        }
        const result = await importRowsStreamingForDe(sdk, {
            deKey: 'K',
            rowSource: src(),
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
    let tmp;
    before(async () => {
        tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-import-'));
    });
    after(async () => {
        await fs.rm(tmp, { recursive: true, force: true });
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

    it('rejects empty CSV', async () => {
        const p = path.join(tmp, 'empty.csv');
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
        const p = path.join(tmp, 'bom.csv');
        await fs.writeFile(p, '\uFEFF', 'utf8');
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
        const p = path.join(tmp, 'header.csv');
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
        const p = path.join(tmp, 'header.tsv');
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
        const p = path.join(tmp, 'empty.json');
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
