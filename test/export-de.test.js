import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import fs from 'node:fs/promises';
import nodeFs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import { Writable } from 'node:stream';
import { setImmediate as immediate } from 'node:timers/promises';
import { createFixtureRoot } from './fixture-root.js';
import {
    fetchAllRowObjects,
    fetchDataExtensionFieldNames,
    serializeRows,
    exportDataExtensionToFile,
    createBackupPathGuard,
} from '../lib/export-de.mjs';

describe('fetchAllRowObjects', () => {
    it('uses getBulk with page size 2500 and maps items to flat rows', async () => {
        const calls = [];
        const sdk = {
            rest: {
                getBulk: async (path, pageSize) => {
                    calls.push({ path, pageSize });
                    assert.ok(path.includes('rowset'));
                    assert.equal(pageSize, 2500);
                    return {
                        items: [
                            {
                                keys: { pk: '1' },
                                values: { a: 'x' },
                            },
                            {
                                keys: { pk: '2' },
                                values: { a: 'y' },
                            },
                        ],
                        count: 2,
                    };
                },
            },
        };
        const rows = await fetchAllRowObjects(sdk, 'DE1');
        assert.deepEqual(rows, [
            { pk: '1', a: 'x' },
            { pk: '2', a: 'y' },
        ]);
        assert.equal(calls.length, 1);
    });
});

describe('serializeRows', () => {
    it('writes CSV with BOM', () => {
        const s = serializeRows([{ a: '1' }], 'csv', false);
        assert.ok(s.startsWith('\u{FEFF}'));
    });

    it('writes CSV with quoted fields', () => {
        const s = serializeRows([{ col1: 'val1', col2: 'val2' }], 'csv', false);
        assert.ok(s.includes('"col1"'), 'CSV headers should be quoted');
        assert.ok(s.includes('"val1"'), 'CSV values should be quoted');
    });

    it('writes TSV without quoted fields', () => {
        const s = serializeRows([{ col1: 'val1', col2: 'val2' }], 'tsv', false);
        assert.ok(!s.includes('"col1"'), 'TSV headers should not be quoted');
        assert.ok(!s.includes('"val1"'), 'TSV values should not be quoted');
        assert.ok(s.includes('col1\tcol2'), 'TSV should have tab-separated headers');
        assert.ok(s.includes('val1\tval2'), 'TSV should have tab-separated values');
    });

    it('writes TSV with BOM', () => {
        const s = serializeRows([{ a: '1' }], 'tsv', false);
        assert.ok(s.startsWith('\u{FEFF}'), 'TSV should start with BOM');
    });

    it('writes CSV header only when rows empty and columns provided', () => {
        const s = serializeRows([], 'csv', false, ['col1', 'col2']);
        assert.ok(s.startsWith('\u{FEFF}'));
        const lines = s.trimEnd().split(/\r?\n/);
        assert.equal(lines.length, 1);
        assert.ok(lines[0].includes('"col1"'));
        assert.ok(lines[0].includes('"col2"'));
    });

    it('writes TSV header only when rows empty and columns provided', () => {
        const s = serializeRows([], 'tsv', false, ['h1', 'h2']);
        assert.ok(s.startsWith('\u{FEFF}'));
        const lines = s.trimEnd().split(/\r?\n/);
        assert.equal(lines.length, 1);
        assert.match(lines[0], /h1\th2/);
    });

    it('JSON ignores columns when rows empty', () => {
        const s = serializeRows([], 'json', false, ['a', 'b']);
        assert.equal(s.trim(), '[]');
    });
});

describe('fetchDataExtensionFieldNames', () => {
    it('sorts by Ordinal and returns Names', async () => {
        const soap = {
            retrieve: async (type, properties, options) => {
                assert.equal(type, 'DataExtensionField');
                assert.deepEqual(properties, ['Name', 'Ordinal']);
                assert.equal(options.filter.leftOperand, 'DataExtension.CustomerKey');
                assert.equal(options.filter.operator, 'equals');
                assert.equal(options.filter.rightOperand, 'MY_DE');
                return {
                    Results: [
                        { Name: 'second', Ordinal: '2' },
                        { Name: 'first', Ordinal: '1' },
                    ],
                };
            },
        };
        const names = await fetchDataExtensionFieldNames(soap, 'MY_DE');
        assert.deepEqual(names, ['first', 'second']);
    });

    it('returns empty array when Results missing or empty', async () => {
        assert.deepEqual(
            await fetchDataExtensionFieldNames({ retrieve: async () => ({ Results: [] }) }, 'K'),
            [],
        );
        assert.deepEqual(
            await fetchDataExtensionFieldNames({ retrieve: async () => ({}) }, 'K'),
            [],
        );
    });
});

const exportParameters = (projectRoot, extra = {}) => ({
    projectRoot,
    credentialName: 'cred',
    buName: 'bu',
    deKey: 'DE_KEY',
    format: 'csv',
    ...extra,
});

// Mock the existing fs boundary, not a new public dependency-injection API.
function mockOutput(t, output) {
    t.mock.method(nodeFs, 'createWriteStream', () => output);
    syncBuiltinESMExports();
    t.after(() => {
        t.mock.restoreAll();
        syncBuiltinESMExports();
    });
}

const onePage = (pageItems) => ({
    rest: {
        async *getBulkPages() {
            yield { pageItems };
        },
    },
});

describe('exportDataExtensionToFile', () => {
    for (const format of ['csv', 'tsv', 'json']) {
        for (const empty of [false, true]) {
            it(`guards every ${format} writer before opening (empty=${empty})`, async () => {
                const root = await createFixtureRoot('mcdata-export-guard-');
                const blocked = new Error('blocked writer');
                const checked = [];
                await assert.rejects(
                    exportDataExtensionToFile(
                        {
                            ...onePage(empty ? [] : [{ keys: { id: '1' } }, { keys: { id: '2' } }]),
                            soap: { retrieve: async () => ({ Results: [] }) },
                        },
                        exportParameters(root, {
                            format,
                            maxRowsPerFile: 1,
                            beforeOpen: async (outPath) => {
                                checked.push(outPath);
                                if (!(empty || checked.length === 2)) {
                                    return;
                                }

                                await fs.writeFile(outPath, 'protected bytes');
                                throw blocked;
                            },
                        }),
                    ),
                    blocked,
                );
                assert.equal(checked.length, empty ? 1 : 2);
                assert.equal(await fs.readFile(checked.at(-1), 'utf8'), 'protected bytes');
            });
        }
    }
    it('rejects resolved directory aliases and hardlinks to frozen inputs', async () => {
        const root = await createFixtureRoot('mcdata-export-alias-');
        const source = `${root}/source.csv`;
        await fs.writeFile(source, 'original');
        const alias = `${root}/alias`;
        await fs.symlink(root, alias, 'junction');
        const hardlink = `${root}/hardlink.csv`;
        await fs.link(source, hardlink);
        const guard = await createBackupPathGuard([source]);
        for (const destination of [
            source,
            `${root}/./source.csv`,
            `${alias}/source.csv`,
            hardlink,
        ]) {
            await assert.rejects(guard(destination), /Backup destination collides/);
        }
        await guard(`${root}/new.csv`);
        assert.equal(await fs.readFile(source, 'utf8'), 'original');
    });
    it(
        'bounds production on slow output and waits for final filesystem completion',
        { timeout: 5000 },
        async (t) => {
            const root = await createFixtureRoot('mcdata-export-slow-');
            let produced = 0;
            let pagesRead = 0;
            let release;
            let finalize;
            const output = new Writable({
                highWaterMark: 1,
                write(_chunk, _encoding, callback) {
                    release = callback;
                },
                final(callback) {
                    finalize = callback;
                },
            });
            mockOutput(t, output);
            const items = Array.from({ length: 500 }, () => ({
                get keys() {
                    produced++;
                    return { value: 'x'.repeat(4096) };
                },
            }));
            const sdk = {
                rest: {
                    async *getBulkPages() {
                        pagesRead++;
                        yield { pageItems: items };
                        pagesRead++;
                    },
                },
            };
            let isSettled = false;
            const exporting = (async () => {
                try {
                    return await exportDataExtensionToFile(sdk, exportParameters(root));
                } finally {
                    isSettled = true;
                }
            })();
            while (!release) {
                await immediate();
            }
            for (let index = 0; index < 10; index++) {
                await immediate();
            }
            assert.ok(produced < 100, `producer ran ${produced} rows ahead`);
            assert.equal(pagesRead, 1);
            assert.equal(isSettled, false);
            while (!finalize) {
                const callback = release;
                release = undefined;
                callback?.();
                await immediate();
            }
            assert.equal(isSettled, false);
            finalize();
            const result = await exporting;
            assert.equal(result.rowCount, 500);
            assert.equal(output.closed, true);
        },
    );

    it('finishes each part before opening its successor', { timeout: 5000 }, async (t) => {
        const root = await createFixtureRoot('mcdata-export-rotation-');
        const finalizers = [];
        const outputs = [];
        t.mock.method(nodeFs, 'createWriteStream', () => {
            const output = new Writable({
                write(_chunk, _encoding, callback) {
                    callback();
                },
                final(callback) {
                    finalizers.push(callback);
                },
            });
            outputs.push(output);
            return output;
        });
        syncBuiltinESMExports();
        t.after(() => {
            t.mock.restoreAll();
            syncBuiltinESMExports();
        });
        const exporting = exportDataExtensionToFile(
            onePage([{ keys: { a: '1' } }, { keys: { a: '2' } }]),
            exportParameters(root, { maxRowsPerFile: 1 }),
        );
        while (finalizers.length === 0) {
            await immediate();
        }
        assert.equal(outputs.length, 1);
        for (let index = 0; index < 5; index++) {
            await immediate();
        }
        assert.equal(outputs.length, 1);
        finalizers[0]();
        while (finalizers.length < 2) {
            await immediate();
        }
        assert.equal(outputs[0].closed, true);
        finalizers[1]();
        assert.equal((await exporting).rowCount, 2);
        assert.equal(outputs[1].closed, true);
    });

    for (const failureMode of [
        'file',
        'drain',
        'close',
        'flush',
        'sdk',
        'stringifier',
        'pending-page',
    ]) {
        it(
            `rejects ${failureMode} failure without hangs or extra reads`,
            { timeout: 5000 },
            async (t) => {
                const root = await createFixtureRoot('mcdata-export-failure-');
                const expected = new Error(`${failureMode} failure`);
                let reads = 0;
                let rejectPage;
                const output = new Writable({
                    highWaterMark: 1,
                    write(_chunk, _encoding, callback) {
                        switch (failureMode) {
                            case 'drain': {
                                queueMicrotask(() => output.destroy(expected));

                                break;
                            }
                            case 'file': {
                                callback(expected);

                                break;
                            }
                            case 'close': {
                                output.destroy();

                                break;
                            }
                            default: {
                                callback();
                            }
                        }
                    },
                    final(callback) {
                        callback(failureMode === 'flush' ? expected : undefined);
                    },
                });
                mockOutput(t, output);
                const sdk = {
                    rest: {
                        getBulkPages() {
                            const iterator = {
                                [Symbol.asyncIterator]() {
                                    return iterator;
                                },
                                next() {
                                    reads++;
                                    if (reads === 1) {
                                        const value =
                                            failureMode === 'stringifier'
                                                ? {
                                                      toJSON() {
                                                          throw expected;
                                                      },
                                                  }
                                                : 'x';
                                        return Promise.resolve({
                                            value: {
                                                pageItems:
                                                    failureMode === 'drain'
                                                        ? Array.from({ length: 100 }, () => ({
                                                              keys: { value: 'x'.repeat(4096) },
                                                          }))
                                                        : [{ keys: { value } }],
                                            },
                                        });
                                    }
                                    if (failureMode === 'sdk') {
                                        return Promise.reject(expected);
                                    }
                                    if (failureMode === 'pending-page') {
                                        queueMicrotask(() => output.destroy(expected));
                                        const pending = Promise.withResolvers();
                                        rejectPage = pending.reject;
                                        return pending.promise;
                                    }
                                    return Promise.resolve({ done: true });
                                },
                                return() {
                                    return new Promise(() => {});
                                },
                            };
                            return iterator;
                        },
                    },
                };
                await assert.rejects(
                    exportDataExtensionToFile(sdk, exportParameters(root)),
                    failureMode === 'close' ? /Premature close/ : expected,
                );
                assert.equal(output.destroyed, true);
                assert.ok(reads <= 2);
                rejectPage?.(new Error('late page rejection'));
                await immediate();
            },
        );
    }

    for (const format of ['csv', 'tsv']) {
        it(`preserves exact ${format} multipart and synchronous bytes`, async () => {
            const root = await createFixtureRoot('mcdata-export-bytes-');
            const rows = [
                { pk: '1', value: 'Ä,"x"\n\t' },
                { pk: '2', value: '' },
                { pk: '3', value: 'last' },
            ];
            const { paths, rowCount } = await exportDataExtensionToFile(
                onePage(rows.map((values) => ({ values }))),
                exportParameters(root, { format, maxRowsPerFile: 2 }),
            );
            const first =
                format === 'csv'
                    ? '\u{FEFF}"pk","value"\n"1","Ä,""x""\n\t"\n"2",\n'
                    : '\u{FEFF}pk\tvalue\n1\t"Ä,""x""\n\t"\n2\t\n';
            const continuation = format === 'csv' ? '"3","last"\n' : '3\tlast\n';
            assert.equal(rowCount, 3);
            assert.match(paths[0], /\.part1\./);
            assert.match(paths[1], /\.part2\./);
            assert.equal(await fs.readFile(paths[0], 'utf8'), first);
            assert.equal(await fs.readFile(paths[1], 'utf8'), continuation);
            assert.equal(serializeRows(rows, format, false), first + continuation);
            const empty = await exportDataExtensionToFile(
                {
                    ...onePage([]),
                    soap: {
                        retrieve: async () => ({
                            Results: [
                                { Name: 'second', Ordinal: 2 },
                                { Name: 'first', Ordinal: 1 },
                            ],
                        }),
                    },
                },
                exportParameters(root, { format, deKey: 'EMPTY' }),
            );
            assert.equal(empty.rowCount, 0);
            assert.equal(
                await fs.readFile(empty.paths[0], 'utf8'),
                format === 'csv' ? '\u{FEFF}"first","second"\n' : '\u{FEFF}first\tsecond\n',
            );
        });
    }

    it('writes header-only CSV for empty DE when SOAP returns fields', async () => {
        const temporary = await createFixtureRoot('mcdata-export-empty-');
        try {
            const sdk = {
                rest: {
                    // eslint-disable-next-line require-yield
                    async *getBulkPages() {
                        throw new Error('Could not find an array to iterate over');
                    },
                },
                soap: {
                    retrieve: async (type, properties) => {
                        assert.equal(type, 'DataExtensionField');
                        assert.deepEqual(properties, ['Name', 'Ordinal']);
                        return {
                            Results: [
                                { Name: 'Email', Ordinal: '1' },
                                { Name: 'Name', Ordinal: '2' },
                            ],
                        };
                    },
                },
            };
            const { paths, rowCount } = await exportDataExtensionToFile(sdk, {
                projectRoot: temporary,
                credentialName: 'cred',
                buName: 'bu',
                deKey: 'DE_KEY',
                format: 'csv',
            });
            assert.equal(rowCount, 0);
            assert.equal(paths.length, 1);
            const body = await fs.readFile(paths[0], 'utf8');
            assert.ok(body.startsWith('\u{FEFF}'));
            assert.ok(body.includes('"Email"'));
            assert.ok(body.includes('"Name"'));
            const lines = body.trimEnd().split(/\r?\n/);
            assert.equal(lines.length, 1);
        } finally {
            // Retain the small, owned synthetic fixture for diagnostics.
        }
    });

    it('streams CSV rows via getBulkPages and returns paths + rowCount', async () => {
        const temporary = await createFixtureRoot('mcdata-export-stream-');
        try {
            const sdk = {
                rest: {
                    async *getBulkPages(url, pageSize) {
                        assert.ok(url.includes('rowset'));
                        assert.equal(pageSize, 2500);
                        yield {
                            iteratorField: 'items',
                            pageItems: [
                                { keys: { pk: '1' }, values: { a: 'x' } },
                                { keys: { pk: '2' }, values: { a: 'y' } },
                            ],
                            page: 1,
                            totalPages: 1,
                            totalCount: 2,
                            responseBatch: { items: [], count: 2 },
                        };
                    },
                },
                soap: {
                    retrieve: async () => ({ Results: [] }),
                },
            };
            const { paths, rowCount } = await exportDataExtensionToFile(sdk, {
                projectRoot: temporary,
                credentialName: 'cred',
                buName: 'bu',
                deKey: 'DE_KEY',
                format: 'csv',
            });
            assert.equal(rowCount, 2);
            assert.equal(paths.length, 1);
            const body = await fs.readFile(paths[0], 'utf8');
            assert.ok(body.includes('"pk"'));
            assert.ok(body.includes('"1"'));
        } finally {
            // Retain the small, owned synthetic fixture for diagnostics.
        }
    });

    it('splits CSV into two parts when maxRowsPerFile is reached', async () => {
        const temporary = await createFixtureRoot('mcdata-export-split-');
        try {
            const sdk = {
                rest: {
                    async *getBulkPages() {
                        yield {
                            iteratorField: 'items',
                            pageItems: [
                                { keys: { pk: '1' }, values: { a: 'x' } },
                                { keys: { pk: '2' }, values: { a: 'y' } },
                                { keys: { pk: '3' }, values: { a: 'z' } },
                            ],
                            page: 1,
                            totalPages: 1,
                            totalCount: 3,
                            responseBatch: { items: [], count: 3 },
                        };
                    },
                },
                soap: {
                    retrieve: async () => ({ Results: [] }),
                },
            };
            const { paths, rowCount } = await exportDataExtensionToFile(sdk, {
                projectRoot: temporary,
                credentialName: 'cred',
                buName: 'bu',
                deKey: 'DE_KEY',
                format: 'csv',
                maxRowsPerFile: 2,
            });
            assert.equal(rowCount, 3);
            assert.equal(paths.length, 2);
        } finally {
            // Retain the small, owned synthetic fixture for diagnostics.
        }
    });
});
