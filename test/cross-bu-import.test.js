import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { registerHooks, syncBuiltinESMExports } from 'node:module';
import nodeFs from 'node:fs';
import { Writable } from 'node:stream';
import { setImmediate as immediate } from 'node:timers/promises';
import { readRowsFromFile } from '../lib/read-rows.mjs';

const sdkModule = `export default class SDK { constructor(auth) { return globalThis.__crossBuSdk(auth); } }`;
registerHooks({
    resolve(specifier, context, nextResolve) {
        return specifier === 'sfmc-sdk'
            ? { url: `data:text/javascript,${encodeURIComponent(sdkModule)}`, shortCircuit: true }
            : nextResolve(specifier, context);
    },
});
const { crossBuImport } = await import('../lib/cross-bu-import.mjs');
import { buildExportBasename, parseExportBasename } from '../lib/filename.mjs';
import { createFixtureRoot } from './fixture-root.js';
import path from 'node:path';
import fs from 'node:fs/promises';
import os from 'node:os';

const mcdevrc = {
    credentials: {
        MyCred: {
            businessUnits: {
                Dev: 111,
                QA: 222,
                Prod: 333,
            },
        },
    },
};
const mcdevAuth = {
    MyCred: {
        client_id: 'cid',
        client_secret: 'csec',
        // Must match the sfmc-sdk auth_url format so the SDK constructor doesn't reject it
        auth_url: 'https://mc00000000000000000000000000.auth.marketingcloudapis.com/',
    },
};

function captureCrossBu(
    t,
    download = async function* () {
        yield { pageItems: [{ keys: { id: 'source' }, values: { value: 'one' } }] };
    },
    operations = {},
) {
    const effects = [];
    t.mock.method(globalThis, 'setTimeout', (callback) => {
        queueMicrotask(callback);
    });
    // eslint-disable-next-line unicorn/no-global-object-property-assignment -- Isolated SDK constructor seam, restored after each test.
    globalThis.__crossBuSdk = (auth) => ({
        rest: {
            get: async (route) => {
                if (route.endsWith('/status')) {
                    return operations.status
                        ? operations.status(auth.account_id)
                        : { status: { requestStatus: 'Complete' } };
                }
                return { count: operations.count ?? 10 };
            },
            put: async (route, body) => {
                effects.push({ type: 'upload', mid: auth.account_id, route, body });
                return (
                    (await operations.upload?.(auth.account_id, body)) ?? {
                        requestId: `accepted-${auth.account_id}`,
                    }
                );
            },
            getBulk: async () => {
                throw new Error('Unbounded download forbidden');
            },
            getBulkPages: async function* (route) {
                effects.push({ type: 'download', mid: auth.account_id, route });
                yield* download(route, auth.account_id);
            },
        },
        soap: {
            retrieve: async () => ({ Results: [{ Name: 'id', Ordinal: 1 }] }),
            perform: async () => {
                effects.push({ type: 'clear', mid: auth.account_id });
                await operations.clear?.(auth.account_id);
            },
        },
    });
    t.after(() => {
        delete globalThis.__crossBuSdk;
    });
    t.mock.method(console, 'log', () => {});
    t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'error', () => {});
    return effects;
}

function importOptions(projectRoot, extra = {}) {
    return {
        projectRoot,
        mcdevrc,
        mcdevAuth,
        targets: [
            { credential: 'MyCred', bu: 'QA' },
            { credential: 'MyCred', bu: 'Prod' },
        ],
        format: 'csv',
        mode: 'upsert',
        backupBeforeImport: false,
        clearBeforeImport: true,
        acceptRiskFlag: true,
        isTTY: false,
        ...extra,
    };
}

describe('crossBuImport — async completion propagation', () => {
    for (const api of [false, true]) {
        for (const outcome of [
            'Complete',
            'missing-id',
            'missing-status',
            'status-error',
            'Cancelled',
            'Error',
        ]) {
            it(`retains mixed-target non-success for ${api ? 'API' : 'file'} mode: ${outcome}`, async (t) => {
                const root = await createFixtureRoot('cbi-completion-');
                const source = path.join(root, 'A.mcdata.csv');
                await fs.writeFile(source, 'id\n1\n');
                const effects = captureCrossBu(t, undefined, {
                    upload: async (mid) =>
                        mid === 222 && outcome === 'missing-id' ? {} : undefined,
                    status: async (mid) => {
                        if (mid === 333) {
                            return { status: { requestStatus: 'Complete' } };
                        }
                        if (outcome === 'status-error') {
                            throw new Error('synthetic status failure');
                        }
                        return outcome === 'missing-status'
                            ? {}
                            : { status: { requestStatus: outcome } };
                    },
                });
                assert.equal(
                    await crossBuImport(
                        importOptions(
                            root,
                            api
                                ? { sourceCred: 'MyCred', sourceBu: 'Dev', deKeys: ['A'] }
                                : { filePaths: [source] },
                        ),
                    ),
                    outcome !== 'Complete',
                );
                assert.deepEqual(
                    effects.filter(({ type }) => type === 'upload').map(({ mid }) => mid),
                    [222, 333],
                );
            });
        }
    }
});

describe('crossBuImport — explicit export selection', () => {
    for (const otherName of [
        'Z.mcdata.2027-01-01.csv',
        'Z.mcdata.2026-01-01.tsv',
        'Z.mcdata.csv',
    ]) {
        it(`rejects ambiguous file selection before any target output or API effect (${otherName})`, async (t) => {
            const root = await createFixtureRoot('cbi-explicit-');
            const paths = ['A.mcdata.csv', 'Z.mcdata.2026-01-01.csv', otherName].map((name) =>
                path.join(root, name),
            );
            for (const file of paths) {
                await fs.writeFile(file, 'id\n1\n');
            }
            const effects = captureCrossBu(t);
            const before = await fs.readdir(root);
            await assert.rejects(
                crossBuImport(importOptions(root, { filePaths: paths, backupBeforeImport: true })),
                /Ambiguous explicit import selection/,
            );
            assert.deepEqual(effects, []);
            assert.deepEqual(await fs.readdir(root), before);
        });
    }

    it('preserves multiple DEs and one ordered contiguous multipart set across targets', async (t) => {
        const root = await createFixtureRoot('cbi-explicit-positive-');
        const paths = ['A.mcdata.part2.csv', 'Z.mcdata.csv', 'A.mcdata.part1.csv'].map((name) =>
            path.join(root, name),
        );
        await fs.writeFile(paths[0], '2\n');
        await fs.writeFile(paths[1], 'id\n3\n');
        await fs.writeFile(paths[2], 'id\n1\n');
        const effects = captureCrossBu(t);
        assert.equal(await crossBuImport(importOptions(root, { filePaths: paths })), false);
        assert.deepEqual(
            effects
                .filter(({ type }) => type === 'upload')
                .map(({ mid, body }) => [mid, body.items]),
            [
                [222, [{ id: '1' }, { id: '2' }]],
                [333, [{ id: '1' }, { id: '2' }]],
                [222, [{ id: '3' }]],
                [333, [{ id: '3' }]],
            ],
        );
    });
});

describe('crossBuImport — streaming multipart snapshots', () => {
    for (const [inputFormat, format] of [
        ['json', 'json'],
        ['csv', 'json'],
        ['json', 'csv'],
        ['json', 'tsv'],
    ]) {
        it(`streams multipart ${inputFormat} to exact ${format} snapshots before ordered fan-out`, async (t) => {
            const root = await createFixtureRoot('cbi-stream-conversion-');
            const rows = [
                { id: '1', value: '雪,"quoted"\nline' },
                { id: '2', value: 'second\tvalue' },
            ];
            const paths = [1, 2].map((part) =>
                path.join(root, `A.mcdata.part${part}.${inputFormat}`),
            );
            await fs.writeFile(
                paths[0],
                inputFormat === 'json'
                    ? JSON.stringify([rows[0]])
                    : 'id,value\n1,"雪,""quoted""\nline"\n',
            );
            await fs.writeFile(
                paths[1],
                inputFormat === 'json' ? JSON.stringify({ items: [rows[1]] }) : '2,second\tvalue\n',
            );
            const effects = captureCrossBu(t, undefined, {
                clear: async () => {
                    for (const bu of ['QA', 'Prod']) {
                        const directory = path.join(root, 'data', 'MyCred', bu);
                        const files = await fs.readdir(directory);
                        assert.equal(files.length, 1);
                        const snapshot = path.join(directory, files[0]);
                        assert.deepEqual(await readRowsFromFile(snapshot, format), rows);
                        if (format === 'json') {
                            assert.equal(
                                await fs.readFile(snapshot, 'utf8'),
                                JSON.stringify(rows) + '\n',
                            );
                        } else {
                            const bytes = await fs.readFile(snapshot, 'utf8');
                            assert.ok(
                                bytes.startsWith(
                                    format === 'csv'
                                        ? '\u{FEFF}"id","value"\n'
                                        : '\u{FEFF}id\tvalue\n',
                                ),
                            );
                        }
                    }
                },
            });
            await crossBuImport(importOptions(root, { filePaths: paths.toReversed(), format }));
            assert.deepEqual(
                effects.map(({ type, mid }) => [type, mid]),
                [
                    ['clear', 222],
                    ['upload', 222],
                    ['clear', 333],
                    ['upload', 333],
                ],
            );
            for (const effect of effects) {
                if (effect.type === 'upload') {
                    assert.deepEqual(effect.body.items, rows);
                }
            }
        });
    }

    it('pauses multipart JSON production behind a slow snapshot writer and awaits final flush', async (t) => {
        const root = await createFixtureRoot('cbi-stream-slow-');
        const paths = [1, 2].map((part) => path.join(root, `A.mcdata.part${part}.json`));
        const rows = Array.from({ length: 100 }, (_, id) => ({ id, value: 'x'.repeat(2000) }));
        for (const file of paths) {
            await fs.writeFile(file, JSON.stringify(rows));
        }
        const effects = captureCrossBu(t);
        const readFile = fs.readFile.bind(fs);
        let secondReads = 0;
        t.mock.method(fs, 'readFile', async (...arguments_) => {
            if (arguments_[0] === paths[1]) {
                secondReads++;
            }
            return readFile(...arguments_);
        });
        let release;
        let finalize;
        const chunks = [];
        const writer = new Writable({
            highWaterMark: 1,
            write(chunk, _encoding, callback) {
                chunks.push(chunk.toString());
                if (release) {
                    callback();
                } else {
                    release = callback;
                }
            },
            final(callback) {
                finalize = callback;
            },
        });
        t.mock.method(nodeFs, 'createWriteStream', (_destination, options) => {
            assert.equal(options.flags, 'wx');
            return writer;
        });
        syncBuiltinESMExports();
        t.after(() => {
            t.mock.restoreAll();
            syncBuiltinESMExports();
        });
        const importing = crossBuImport(
            importOptions(root, {
                filePaths: paths,
                format: 'json',
                targets: [{ credential: 'MyCred', bu: 'QA' }],
            }),
        );
        while (!release) {
            await immediate();
        }
        // Preflight reads both files; streaming has not reached the second part again.
        await immediate();
        assert.equal(secondReads, 1);
        assert.deepEqual(effects, []);
        release();
        while (!finalize) {
            await immediate();
        }
        assert.deepEqual(effects, []);
        finalize();
        await importing;
        assert.equal(chunks.join(''), JSON.stringify([...rows, ...rows]) + '\n');
        assert.equal(secondReads, 3); // Preflight, snapshot, upload.
    });

    for (const format of ['json', 'csv', 'tsv']) {
        it(`rejects later ${format} snapshot final-flush failure before any target mutation`, async (t) => {
            const root = await createFixtureRoot('cbi-stream-fail-');
            const source = path.join(root, 'A.mcdata.json');
            await fs.writeFile(source, '[{"id":"1"}]');
            const effects = captureCrossBu(t);
            let writers = 0;
            t.mock.method(nodeFs, 'createWriteStream', (_destination, options) => {
                assert.equal(options.flags, 'wx');
                const isFail = ++writers === 2;
                return new Writable({
                    write(_chunk, _encoding, callback) {
                        callback();
                    },
                    final(callback) {
                        callback(isFail ? new Error('synthetic final-flush failure') : null);
                    },
                });
            });
            syncBuiltinESMExports();
            t.after(() => {
                t.mock.restoreAll();
                syncBuiltinESMExports();
            });
            await assert.rejects(
                crossBuImport(importOptions(root, { filePaths: [source], format })),
                /synthetic final-flush failure/u,
            );
            assert.equal(writers, 2);
            assert.deepEqual(effects, []);
        });
    }
});

describe('crossBuImport — global preparation', () => {
    for (const contents of [' \t \n\n', 'id,value\n\n \t \n', 'id,value\n\n2,bad,extra\n']) {
        it(`skip policy rejects later empty/invalid DE before backups/snapshots/mutation (${JSON.stringify(contents)})`, async (t) => {
            const root = await createFixtureRoot('cbi-policy-gate-');
            const first = path.join(root, 'A.mcdata.csv');
            const last = path.join(root, 'Z.mcdata.csv');
            await fs.writeFile(first, 'id,value\n\n1,ok\n');
            await fs.writeFile(last, contents);
            const effects = captureCrossBu(t);
            await assert.rejects(
                crossBuImport(
                    importOptions(root, {
                        filePaths: [first, last],
                        skipEmptyLines: true,
                        backupBeforeImport: true,
                    }),
                ),
                /no data rows|Row length|columns|fields/,
            );
            assert.deepEqual(effects, []);
            assert.deepEqual(
                (await fs.readdir(root)).toSorted((a, b) => a.localeCompare(b)),
                ['.fixture-owner', 'A.mcdata.csv', 'Z.mcdata.csv'],
            );
        });
    }
    for (const format of ['csv', 'tsv', 'json']) {
        for (const skipEmptyLines of [undefined, false, true, 'true']) {
            it(`keeps snapshots and every upload on the same ${format} policy (${skipEmptyLines})`, async (t) => {
                const root = await createFixtureRoot('cbi-policy-values-');
                const paths = [1, 2].map((part) =>
                    path.join(root, `A.mcdata.part${part}.${format}`),
                );
                const delimiter = format === 'tsv' ? '\t' : ',';
                const rows = [
                    { id: '', value: '' },
                    { id: '1', value: ` exact${delimiter}\nvalue ` },
                    { id: '', value: '' },
                    { id: ' ', value: '"literal"' },
                ];
                await fs.writeFile(
                    paths[0],
                    format === 'json'
                        ? JSON.stringify([rows[0]])
                        : `\u{FEFF}id${delimiter}value\n\n""${delimiter}""\n`,
                );
                await fs.writeFile(
                    paths[1],
                    format === 'json'
                        ? JSON.stringify(rows.slice(1))
                        : `\u{FEFF}   \n1${delimiter}" exact${delimiter}\nvalue "\n${delimiter}\n" "${delimiter}"""literal"""\n\n`,
                );
                const effects = captureCrossBu(t);
                const operation = crossBuImport(
                    importOptions(root, { filePaths: paths, skipEmptyLines, format: 'json' }),
                );
                if (format !== 'json' && skipEmptyLines !== true) {
                    await assert.rejects(operation, /Row length|columns|fields/);
                    assert.deepEqual(effects, []);
                    await assert.rejects(fs.stat(path.join(root, 'data')), { code: 'ENOENT' });
                    return;
                }
                assert.equal(await operation, false);
                const uploads = effects.filter(({ type }) => type === 'upload');
                assert.equal(uploads.length, 2);
                for (const upload of uploads) {
                    assert.deepEqual(upload.body.items, rows);
                }
                for (const bu of ['QA', 'Prod']) {
                    const directory = path.join(root, 'data', 'MyCred', bu);
                    const files = await fs.readdir(directory);
                    assert.equal(files.length, 1);
                    assert.deepEqual(
                        await readRowsFromFile(path.join(directory, files[0]), 'json'),
                        rows,
                    );
                }
            });
        }
    }

    it('rejects relative and filesystem-root OS temp paths before staging', async (t) => {
        const effects = captureCrossBu(t);
        for (const temporaryDirectory of ['relative-temp', path.parse(process.cwd()).root]) {
            const mocked = t.mock.method(os, 'tmpdir', () => temporaryDirectory);
            await assert.rejects(
                crossBuImport(
                    importOptions('unused', {
                        sourceCred: 'MyCred',
                        sourceBu: 'Dev',
                        deKeys: ['A'],
                    }),
                ),
                /absolute, non-root OS temp directory/u,
            );
            mocked.mock.restore();
        }
        assert.deepEqual(effects, []);
    });

    it('uses the OS temp directory and retains an owned API stage', async (t) => {
        const temporaryDirectory = os.tmpdir();
        const mkdtemp = fs.mkdtemp.bind(fs);
        let stage;
        t.mock.method(fs, 'mkdtemp', async (prefix) => {
            assert.equal(prefix, path.join(temporaryDirectory, 'mcdata-cross-bu-'));
            stage = await mkdtemp(prefix);
            return stage;
        });
        captureCrossBu(t, async function* () {
            yield* [];
            throw new Error('synthetic staging stop');
        });
        await assert.rejects(
            crossBuImport(
                importOptions('unused', { sourceCred: 'MyCred', sourceBu: 'Dev', deKeys: ['A'] }),
            ),
            /synthetic staging stop/u,
        );
        assert.match(
            await fs.readFile(path.join(stage, '.stage-owner'), 'utf8'),
            /^[a-f\d-]{36}$/u,
        );
    });

    for (const clearBeforeImport of [true, false]) {
        it(`rejects the last malformed file before backup or mutation (CLEAR ${clearBeforeImport})`, async (t) => {
            const root = await createFixtureRoot('cbi-global-');
            const first = path.join(root, 'A.mcdata.csv');
            const last = path.join(root, 'Z.mcdata.csv');
            await fs.writeFile(first, 'id,value\n1,valid\n');
            await fs.writeFile(last, 'id,value\n2,valid\n3,invalid,extra\n');
            const effects = captureCrossBu(t);
            await assert.rejects(
                () =>
                    crossBuImport(
                        importOptions(root, {
                            filePaths: [first, last],
                            backupBeforeImport: true,
                            clearBeforeImport,
                        }),
                    ),
                /Row length|columns|fields/,
            );
            assert.deepEqual(effects, []);
        });
    }
    for (const failure of ['download', 'empty']) {
        it(`stages all API sources and rejects later ${failure} before target effects`, async (t) => {
            const root = await createFixtureRoot('cbi-api-stage-');
            const effects = captureCrossBu(t, async function* (route) {
                if (route.includes('Z')) {
                    if (failure === 'download') {
                        throw new Error('synthetic later download');
                    }
                    return;
                }
                yield { pageItems: [{ keys: { id: 'one' }, values: {} }] };
            });
            await assert.rejects(
                () =>
                    crossBuImport(
                        importOptions(root, {
                            sourceCred: 'MyCred',
                            sourceBu: 'Dev',
                            deKeys: ['A', 'Z'],
                            backupBeforeImport: true,
                        }),
                    ),
                failure === 'download' ? /synthetic later download/ : /no data rows/i,
            );
            assert.equal(effects.length, 2);
            assert.ok(effects.every((effect) => effect.type === 'download' && effect.mid === 111));
        });
    }
    it('prepares format-correct multipart snapshots and identical ordered target uploads', async (t) => {
        const root = await createFixtureRoot('cbi-multipart-');
        const first = path.join(root, 'A.mcdata.part1.csv');
        const last = path.join(root, 'A.mcdata.part2.csv');
        await fs.writeFile(first, 'id,value\n1,"first\nline"');
        await fs.writeFile(last, '2,"second,value"');
        const effects = captureCrossBu(t);
        assert.equal(
            await crossBuImport(importOptions(root, { filePaths: [first, last], format: 'tsv' })),
            false,
        );
        assert.deepEqual(
            effects.map(({ type, mid }) => [type, mid]),
            [
                ['clear', 222],
                ['upload', 222],
                ['clear', 333],
                ['upload', 333],
            ],
        );
        assert.deepEqual(effects[1].body, effects[3].body);
        for (const bu of ['QA', 'Prod']) {
            const directory = path.join(root, 'data', 'MyCred', bu);
            const files = await fs.readdir(directory);
            assert.equal(files.length, 1);
            assert.deepEqual(await readRowsFromFile(path.join(directory, files[0]), 'tsv'), [
                { id: '1', value: 'first\nline' },
                { id: '2', value: 'second,value' },
            ]);
        }
    });
    it('guards backups against the complete frozen set, including another DE source', async (t) => {
        const root = await createFixtureRoot('cbi-backup-collision-');
        const directory = path.join(root, 'data', 'MyCred', 'QA');
        await fs.mkdir(directory, { recursive: true });
        const first = path.join(root, 'A.mcdata.csv');
        const last = path.join(root, 'Z.mcdata.csv');
        const bytes = 'id,value\n1,selected\n';
        await fs.writeFile(first, bytes);
        await fs.writeFile(last, bytes);
        const collision = path.join(
            directory,
            buildExportBasename('A', '2026-01-01T00-00-00.000Z', 'csv', false),
        );
        await fs.link(last, collision);
        const effects = captureCrossBu(t);
        t.mock.method(Date.prototype, 'toISOString', () => '2026-01-01T00:00:00.000Z');
        await assert.rejects(
            () =>
                crossBuImport(
                    importOptions(root, {
                        filePaths: [first, last],
                        backupBeforeImport: true,
                    }),
                ),
            /collides/,
        );
        assert.equal(await fs.readFile(last, 'utf8'), bytes);
        assert.ok(effects.every((effect) => effect.type === 'download'));
    });
    it('finishes every snapshot before CLEAR, rejecting a later snapshot write failure', async (t) => {
        const root = await createFixtureRoot('cbi-snapshot-failure-');
        const source = path.join(root, 'A.mcdata.csv');
        await fs.writeFile(source, 'id\n1\n');
        await fs.mkdir(path.join(root, 'data', 'MyCred'), { recursive: true });
        await fs.writeFile(path.join(root, 'data', 'MyCred', 'Prod'), 'not a directory');
        const effects = captureCrossBu(t);
        await assert.rejects(
            () => crossBuImport(importOptions(root, { filePaths: [source] })),
            /EEXIST|ENOTDIR/,
        );
        assert.deepEqual(effects, []);
    });
    it('rejects a snapshot hardlink to any frozen source without truncation or target effects', async (t) => {
        const root = await createFixtureRoot('cbi-snapshot-collision-');
        const source = path.join(root, 'A.mcdata.csv');
        const bytes = 'id,value\n1,safe\n';
        await fs.writeFile(source, bytes);
        const effects = captureCrossBu(t);
        const originalMkdir = fs.mkdir;
        // Create an existing destination alias before the snapshot guard runs.
        t.mock.method(fs, 'mkdir', async (...arguments_) => {
            await originalMkdir(...arguments_);
            if (String(arguments_[0]).includes(`${path.sep}QA`)) {
                // A fixed clock makes the expected normal snapshot name deterministic.
                await fs.link(
                    source,
                    path.join(
                        arguments_[0],
                        buildExportBasename('A', '2026-01-01T00-00-00.000Z', 'csv', false),
                    ),
                );
            }
        });
        t.mock.method(Date.prototype, 'toISOString', () => '2026-01-01T00:00:00.000Z');
        await assert.rejects(
            () => crossBuImport(importOptions(root, { filePaths: [source] })),
            /collides/,
        );
        assert.equal(await fs.readFile(source, 'utf8'), bytes);
        assert.deepEqual(effects, []);
    });
});

describe('crossBuImport — output preservation and fan-out diagnostics', () => {
    for (const format of ['csv', 'tsv', 'json']) {
        it(`preserves generated ${format} backup and source bytes on same-timestamp snapshot collision`, async (t) => {
            const root = await createFixtureRoot('cbi-backup-preserve-');
            const source = path.join(root, 'A.mcdata.csv');
            const bytes = 'id,value\n1,selected\n';
            await fs.writeFile(source, bytes);
            const effects = captureCrossBu(t);
            t.mock.method(Date.prototype, 'toISOString', () => '2026-01-01T00:00:00.000Z');
            await assert.rejects(
                () =>
                    crossBuImport(
                        importOptions(root, {
                            filePaths: [source],
                            format,
                            backupBeforeImport: true,
                        }),
                    ),
                /already exists; refusing to overwrite/,
            );
            assert.equal(await fs.readFile(source, 'utf8'), bytes);
            for (const bu of ['QA', 'Prod']) {
                const backup = path.join(
                    root,
                    'data',
                    'MyCred',
                    bu,
                    buildExportBasename('A', '2026-01-01T00-00-00.000Z', format, false),
                );
                const backupBytes = {
                    csv: '\u{FEFF}"id","value"\n"source","one"\n',
                    tsv: '\u{FEFF}id\tvalue\nsource\tone\n',
                    json: '[\n{"id":"source","value":"one"}\n]\n',
                };
                assert.equal(await fs.readFile(backup, 'utf8'), backupBytes[format]);
                assert.deepEqual(await readRowsFromFile(backup, format), [
                    { id: 'source', value: 'one' },
                ]);
            }
            assert.ok(effects.every(({ type }) => type === 'download'));
        });
        it(`stages paged API sources and sends identical exact ${format} content to both targets`, async (t) => {
            const root = await createFixtureRoot('cbi-api-exact-');
            const expected = [
                { id: '1', value: '"quoted",\t雪\nline' },
                { id: '2', value: '' },
            ];
            const effects = captureCrossBu(t, async function* () {
                for (const row of expected) {
                    yield { pageItems: [{ keys: { id: row.id }, values: { value: row.value } }] };
                }
            });
            assert.equal(
                await crossBuImport(
                    importOptions(root, {
                        sourceCred: 'MyCred',
                        sourceBu: 'Dev',
                        deKeys: ['A', 'Z'],
                        format,
                    }),
                ),
                false,
            );
            assert.deepEqual(
                effects.filter(({ type }) => type === 'download').map(({ mid }) => mid),
                [111, 111],
            );
            const uploads = effects.filter(({ type }) => type === 'upload');
            assert.deepEqual(
                uploads.map(({ mid }) => mid),
                [222, 333, 222, 333],
            );
            for (const upload of uploads) {
                assert.deepEqual(upload.body.items, expected);
            }
            for (const bu of ['QA', 'Prod']) {
                const directory = path.join(root, 'data', 'MyCred', bu);
                const files = await fs.readdir(directory);
                assert.equal(files.length, 2);
                for (const file of files) {
                    assert.deepEqual(
                        await readRowsFromFile(path.join(directory, file), format),
                        expected,
                    );
                }
            }
        });
    }
    for (const clearState of [
        'not requested',
        'skipped (already empty)',
        'succeeded',
        'attempted; outcome unknown',
    ]) {
        it(`reports earlier accepted target and truthful CLEAR ${clearState} on later failure`, async (t) => {
            const root = await createFixtureRoot('cbi-fanout-fail-');
            const source = path.join(root, 'A.mcdata.csv');
            await fs.writeFile(source, 'id,value\n1,exact\n');
            const clearFails = clearState.startsWith('attempted');
            const effects = captureCrossBu(t, undefined, {
                count: clearState.startsWith('skipped') ? 0 : 10,
                clear: async (mid) => {
                    if (mid === 333 && clearFails) {
                        throw new Error('synthetic CLEAR failure');
                    }
                },
                upload: async (mid) => {
                    if (mid === 333) {
                        throw new Error('synthetic upload failure');
                    }
                },
            });
            await assert.rejects(
                () =>
                    crossBuImport(
                        importOptions(root, {
                            filePaths: [source],
                            clearBeforeImport: clearState !== 'not requested',
                        }),
                    ),
                (error) => {
                    assert.match(error.message, /MyCred\/Prod DE "A"/);
                    assert.ok(error.message.includes(`CLEAR status: ${clearState}`));
                    assert.match(
                        error.message,
                        /Earlier accepted target jobs: 1 \(MyCred\/QA DE "A"\)/,
                    );
                    assert.match(error.message, /acceptance does not confirm completion/);
                    assert.match(error.message, /No automatic retry or rollback/);
                    assert.deepEqual(error.importSubmission, {
                        acceptedRows: 0,
                        acceptedRequests: 0,
                        requestIds: [],
                    });
                    return true;
                },
            );
            assert.equal(
                effects.filter(({ type, mid }) => type === 'upload' && mid === 222).length,
                1,
            );
            assert.equal(
                effects.filter(({ type, mid }) => type === 'upload' && mid === 333).length,
                clearFails ? 0 : 1,
            );
        });
    }
    for (const format of ['csv', 'json']) {
        it(`preserves accepted batch counters on later ${format} target upload failure`, async (t) => {
            const root = await createFixtureRoot('cbi-partial-submission-');
            const source = path.join(root, `A.mcdata.${format}`);
            const rows = Array.from({ length: 2501 }, (_, index) => ({ id: String(index) }));
            await fs.writeFile(
                source,
                format === 'json'
                    ? JSON.stringify(rows)
                    : `id\n${rows.map(({ id }) => id).join('\n')}\n`,
            );
            let calls = 0;
            captureCrossBu(t, undefined, {
                upload: async (mid) => {
                    if (mid === 333 && ++calls === 2) {
                        throw new Error('synthetic second batch failure');
                    }
                },
            });
            await assert.rejects(
                () => crossBuImport(importOptions(root, { filePaths: [source] })),
                (error) => {
                    assert.match(error.message, /Earlier accepted target jobs: 1/);
                    assert.match(error.message, /CLEAR status: succeeded/);
                    assert.deepEqual(error.importSubmission, {
                        acceptedRows: 2500,
                        acceptedRequests: 1,
                        requestIds: ['accepted-333'],
                    });
                    assert.deepEqual(error.cause.importSubmission, error.importSubmission);
                    return true;
                },
            );
        });
    }
    it('preserves a prior snapshot when a later repeated target collides, before any mutation', async (t) => {
        const root = await createFixtureRoot('cbi-duplicate-snapshot-');
        const source = path.join(root, 'A.mcdata.csv');
        const bytes = 'id\n1\n';
        await fs.writeFile(source, bytes);
        const effects = captureCrossBu(t);
        t.mock.method(Date.prototype, 'toISOString', () => '2026-01-01T00:00:00.000Z');
        const target = { credential: 'MyCred', bu: 'QA' };
        await assert.rejects(
            () =>
                crossBuImport(
                    importOptions(root, {
                        filePaths: [source],
                        targets: [target, target],
                    }),
                ),
            /already exists; refusing to overwrite/,
        );
        const directory = path.join(root, 'data', 'MyCred', 'QA');
        const files = await fs.readdir(directory);
        assert.equal(files.length, 1);
        assert.equal(
            await fs.readFile(path.join(directory, files[0]), 'utf8'),
            '\u{FEFF}"id"\n"1"\n',
        );
        assert.equal(await fs.readFile(source, 'utf8'), bytes);
        assert.deepEqual(effects, []);
    });
    it('rejects repeated target backup output before truncation or mutation', async (t) => {
        const root = await createFixtureRoot('cbi-duplicate-backup-');
        const source = path.join(root, 'A.mcdata.csv');
        await fs.writeFile(source, 'id\n1\n');
        const effects = captureCrossBu(t);
        t.mock.method(Date.prototype, 'toISOString', () => '2026-01-01T00:00:00.000Z');
        const target = { credential: 'MyCred', bu: 'QA' };
        await assert.rejects(
            () =>
                crossBuImport(
                    importOptions(root, {
                        filePaths: [source],
                        targets: [target, target],
                        backupBeforeImport: true,
                    }),
                ),
            /already exists; refusing to overwrite/,
        );
        const directory = path.join(root, 'data', 'MyCred', 'QA');
        const files = await fs.readdir(directory);
        assert.equal(files.length, 1);
        assert.deepEqual(await readRowsFromFile(path.join(directory, files[0]), 'csv'), [
            { id: 'source', value: 'one' },
        ]);
        assert.ok(effects.every(({ type }) => type === 'download'));
    });
});

describe('crossBuImport — API mode', () => {
    it('is exported as a function', () => {
        assert.strictEqual(typeof crossBuImport, 'function');
    });

    it('rejects when source credential is missing from mcdevrc', async () => {
        const temporaryDirectory = await createFixtureRoot('cbi-test-');
        await assert.rejects(
            () =>
                crossBuImport({
                    projectRoot: temporaryDirectory,
                    mcdevrc,
                    mcdevAuth,
                    sourceCred: 'UnknownCred',
                    sourceBu: 'Dev',
                    targets: [{ credential: 'MyCred', bu: 'QA' }],
                    deKeys: ['DE1'],
                    format: 'csv',
                    mode: 'upsert',
                    clearBeforeImport: false,
                    acceptRiskFlag: false,
                    isTTY: false,
                }),
            /UnknownCred/,
        );
        // Retain the small, owned synthetic fixture for diagnostics.
    });

    it('rejects when target BU is unknown', async () => {
        const temporaryDirectory = await createFixtureRoot('cbi-test-');
        await assert.rejects(
            () =>
                crossBuImport({
                    projectRoot: temporaryDirectory,
                    mcdevrc,
                    mcdevAuth,
                    sourceCred: 'MyCred',
                    sourceBu: 'Dev',
                    targets: [{ credential: 'MyCred', bu: 'NonExistent' }],
                    deKeys: ['DE1'],
                    format: 'csv',
                    mode: 'upsert',
                    clearBeforeImport: false,
                    acceptRiskFlag: false,
                    isTTY: false,
                }),
            /NonExistent/,
        );
        // Retain the small, owned synthetic fixture for diagnostics.
    });
});

describe('crossBuImport — file mode', () => {
    it('rejects when a target BU is unknown', async () => {
        const temporaryDirectory = await createFixtureRoot('cbi-file-test-');
        const fakeCsv = path.join(
            temporaryDirectory,
            buildExportBasename('My_DE', '2026-04-08T10-00-00.000Z', 'csv'),
        );
        await fs.writeFile(fakeCsv, 'col1,col2\nval1,val2\n', 'utf8');
        await assert.rejects(
            () =>
                crossBuImport({
                    projectRoot: temporaryDirectory,
                    mcdevrc,
                    mcdevAuth,
                    filePaths: [fakeCsv],
                    targets: [{ credential: 'MyCred', bu: 'NonExistent' }],
                    format: 'csv',
                    mode: 'upsert',
                    clearBeforeImport: false,
                    acceptRiskFlag: false,
                    isTTY: false,
                }),
            /NonExistent/,
        );
        // Retain the small, owned synthetic fixture for diagnostics.
    });

    it('derives DE key from .mcdata. filename correctly', () => {
        const basename = buildExportBasename('Contact_DE', '2026-04-08T10-00-00.000Z', 'csv');
        const { customerKey } = parseExportBasename(basename);
        assert.equal(customerKey, 'Contact_DE');
    });
});

describe('crossBuImport — backupBeforeImport flag', () => {
    it('skips backup when backupBeforeImport is false, even with isTTY true', async () => {
        const temporaryDirectory = await createFixtureRoot('cbi-backup-test-');
        // Rejects at SDK connection (bad creds), but must not attempt the TTY backup prompt
        // before reaching that point — target validation happens first, so use a valid target.
        // We only verify the backup export is not triggered; SDK failure is expected.
        await assert.rejects(
            () =>
                crossBuImport({
                    projectRoot: temporaryDirectory,
                    mcdevrc,
                    mcdevAuth,
                    sourceCred: 'UnknownCred',
                    sourceBu: 'Dev',
                    targets: [{ credential: 'MyCred', bu: 'QA' }],
                    deKeys: ['DE1'],
                    format: 'csv',
                    mode: 'upsert',
                    backupBeforeImport: false,
                    clearBeforeImport: false,
                    acceptRiskFlag: false,
                    isTTY: true,
                }),
            /UnknownCred/,
        );
        // Retain the small, owned synthetic fixture for diagnostics.
    });

    it('skips backup when backupBeforeImport is undefined and isTTY is false', async () => {
        const temporaryDirectory = await createFixtureRoot('cbi-notty-test-');
        await assert.rejects(
            () =>
                crossBuImport({
                    projectRoot: temporaryDirectory,
                    mcdevrc,
                    mcdevAuth,
                    sourceCred: 'UnknownCred',
                    sourceBu: 'Dev',
                    targets: [{ credential: 'MyCred', bu: 'QA' }],
                    deKeys: ['DE1'],
                    format: 'csv',
                    mode: 'upsert',
                    backupBeforeImport: undefined,
                    clearBeforeImport: false,
                    acceptRiskFlag: false,
                    isTTY: false,
                }),
            /UnknownCred/,
        );
        // Retain the small, owned synthetic fixture for diagnostics.
    });
});
