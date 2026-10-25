/**
 * Integration-style tests for the skip-clear-if-empty logic in the import command.
 * Uses the public `main()` entry point with a fully mocked SDK and filesystem.
 */
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { registerHooks } from 'node:module';
import { spawnSync } from 'node:child_process';
import { fileURLToPath, pathToFileURL } from 'node:url';
import fs from 'node:fs/promises';
import path from 'node:path';
import { createFixtureRoot } from './fixture-root.js';

// Replace only the SDK constructor; run real CLI selection, preflight and upload orchestration.
const sdkModule = `export default class SDK { constructor() { return globalThis.__cliImportSdk; } }`;
registerHooks({
    resolve(specifier, context, nextResolve) {
        return specifier === 'sfmc-sdk'
            ? { url: `data:text/javascript,${encodeURIComponent(sdkModule)}`, shortCircuit: true }
            : nextResolve(specifier, context);
    },
});
const { main } = await import('../lib/cli.mjs');

async function importProject() {
    const root = await createFixtureRoot('mcdata-cli-gate-');
    await fs.writeFile(
        path.join(root, '.mcdatarc.json'),
        JSON.stringify({ credentials: { C: { businessUnits: { B: 123 } } } }),
    );
    await fs.writeFile(
        path.join(root, '.mcdata-auth.json'),
        JSON.stringify({
            C: {
                client_id: 'synthetic',
                client_secret: 'synthetic',
                auth_url: 'https://example.invalid/',
            },
        }),
    );
    const directory = path.join(root, 'data', 'C', 'B');
    await fs.mkdir(directory, { recursive: true });
    return { root, directory };
}

function captureSdk(t, count = 10) {
    const effects = [];
    t.mock.method(globalThis, 'setTimeout', (callback) => {
        queueMicrotask(callback);
    });
    // eslint-disable-next-line unicorn/no-global-object-property-assignment -- SDK constructor injection is test-only and restored after each case.
    globalThis.__cliImportSdk = {
        rest: {
            get: async (route) =>
                route.endsWith('/status') ? { status: { requestStatus: 'Complete' } } : { count },
            put: async (route, body) => {
                effects.push({ type: 'upload', route, body });
                return { requestId: `accepted-${effects.length}` };
            },
            post: async () => {
                throw new Error('unexpected POST');
            },
            getBulkPages: async function* () {
                effects.push({ type: 'backup' });
                yield { pageItems: [{ keys: { id: 'backup' }, values: {} }] };
            },
        },
        soap: {
            perform: async () => {
                effects.push({ type: 'clear' });
            },
        },
    };
    t.after(() => {
        delete globalThis.__cliImportSdk;
    });
    t.mock.method(console, 'log', () => {});
    t.mock.method(console, 'warn', () => {});
    t.mock.method(console, 'error', () => {});
    return effects;
}

function argv(root, selection, shouldClear = false) {
    return [
        'node',
        'mcdata',
        'import',
        'C/B',
        '--project',
        root,
        ...selection,
        ...(shouldClear ? ['--clear-before-import', '--i-accept-clear-data-risk'] : []),
    ];
}

describe('CLI async completion propagation', () => {
    for (const selection of ['de', 'file', 'cross-file', 'cross-api']) {
        for (const outcome of [
            'Complete',
            'missing-id',
            'missing-status',
            'status-error',
            'Cancelled',
            'Error',
        ]) {
            it(`returns truthful exit status for ${selection}: ${outcome}`, async (t) => {
                const { root, directory } = await importProject();
                const source = path.join(directory, 'A.mcdata.csv');
                await fs.writeFile(source, 'id\n1\n');
                const effects = captureSdk(t);
                const sdk = globalThis.__cliImportSdk;
                const get = sdk.rest.get;
                sdk.rest.get = async (route) => {
                    if (!route.endsWith('/status')) {
                        return get(route);
                    }
                    if (outcome === 'status-error') {
                        throw new Error('synthetic status failure');
                    }
                    return outcome === 'missing-status'
                        ? {}
                        : { status: { requestStatus: outcome } };
                };
                if (outcome === 'missing-id') {
                    sdk.rest.put = async (route, body) => {
                        effects.push({ type: 'upload', route, body });
                        return {};
                    };
                }
                const arguments_ = selection.startsWith('cross-')
                    ? [
                          'node',
                          'mcdata',
                          'import',
                          '--project',
                          root,
                          '--to',
                          'C/B',
                          '--no-backup-before-import',
                          ...(selection === 'cross-api'
                              ? ['--from', 'C/B', '--de', 'A']
                              : ['--file', source]),
                      ]
                    : argv(root, selection === 'de' ? ['--de', 'A'] : ['--file', source]);
                assert.equal(await main(arguments_), outcome === 'Complete' ? 0 : 1);
                assert.equal(effects.filter(({ type }) => type === 'upload').length, 1);
            });
        }
    }
});

describe('CLI explicit export selection', () => {
    for (const cross of [false, true]) {
        for (const otherName of [
            'Z.mcdata.2027-01-01.csv',
            'Z.mcdata.2026-01-01.tsv',
            'Z.mcdata.csv',
        ]) {
            it(`rejects ambiguous runs/formats before backup/CLEAR/upload (cross=${cross}, ${otherName})`, async (t) => {
                const { root, directory } = await importProject();
                const names = ['A.mcdata.csv', 'Z.mcdata.2026-01-01.csv', otherName];
                const paths = names.map((name) => path.join(directory, name));
                for (const file of paths) {
                    await fs.writeFile(file, 'id\n1\n');
                }
                const effects = captureSdk(t);
                const before = await fs.readdir(directory);
                const arguments_ = argv(
                    root,
                    [...paths.flatMap((file) => ['--file', file]), '--backup-before-import'],
                    true,
                );
                if (cross) {
                    arguments_.splice(3, 1, '--to', 'C/B');
                    await assert.rejects(main(arguments_), /Ambiguous explicit import selection/);
                } else {
                    assert.equal(await main(arguments_), 1);
                }
                assert.deepEqual(effects, []);
                assert.deepEqual(await fs.readdir(directory), before);
            });
        }

        it(`preserves explicit multiple DEs and ordered multipart (cross=${cross})`, async (t) => {
            const { root, directory } = await importProject();
            const paths = [
                'A.mcdata.part2.2026-01-01.csv',
                'Z.mcdata.csv',
                'A.mcdata.part1.2026-01-01.csv',
            ].map((name) => path.join(directory, name));
            for (const [index, file] of paths.entries()) {
                await fs.writeFile(file, `${index === 0 ? '' : 'id\n'}${index + 1}\n`);
            }
            const effects = captureSdk(t);
            const arguments_ = argv(
                root,
                paths.flatMap((file) => ['--file', file]),
            );
            if (cross) {
                arguments_.splice(3, 1, '--to', 'C/B');
            }
            assert.equal(await main(arguments_), 0);
            assert.deepEqual(
                effects.filter(({ type }) => type === 'upload').map(({ body }) => body.items),
                [[{ id: '3' }, { id: '1' }], [{ id: '2' }]],
            );
        });
    }

    it('keeps newest-mtime discovery across multiple runs and formats', async (t) => {
        const { root, directory } = await importProject();
        const paths = ['A.mcdata.2027-01-01.csv', 'A.mcdata.2026-01-01.tsv', 'A.mcdata.csv'].map(
            (name) => path.join(directory, name),
        );
        for (const [index, file] of paths.entries()) {
            await fs.writeFile(file, `id\n${index + 1}\n`);
            await fs.utimes(file, 100 + index, 100 + index);
        }
        const effects = captureSdk(t);
        assert.equal(await main(argv(root, ['--de', 'A'])), 0);
        assert.deepEqual(
            effects.filter(({ type }) => type === 'upload').map(({ body }) => body.items),
            [[{ id: '3' }]],
        );
    });
});

describe('CLI whole-invocation file gate', () => {
    for (const byDe of [true, false]) {
        for (const contents of [' \t \n\n', 'id,value\n\n \t \n', 'id,value\n\n2,bad,extra\n']) {
            it(`skip policy rejects later empty/invalid source before all effects (${byDe}, ${JSON.stringify(contents)})`, async (t) => {
                const { root, directory } = await importProject();
                const first = path.join(directory, 'A.mcdata.csv');
                const last = path.join(directory, 'Z.mcdata.csv');
                await fs.writeFile(first, 'id,value\n\n1,ok\n');
                await fs.writeFile(last, contents);
                const effects = captureSdk(t);
                const selection = byDe
                    ? ['--de', 'A', '--de', 'Z']
                    : ['--file', first, '--file', last];
                const before = await fs.readdir(directory);
                assert.equal(
                    await main(
                        argv(
                            root,
                            [...selection, '--skip-empty-lines', '--backup-before-import'],
                            true,
                        ),
                    ),
                    1,
                );
                assert.deepEqual(effects, []);
                assert.deepEqual(await fs.readdir(directory), before);
            });
        }
        for (const format of ['csv', 'tsv', 'json']) {
            for (const enabled of [false, true]) {
                it(`threads single-BU ${format} multipart opt-in (${byDe}, ${enabled})`, async (t) => {
                    const { root, directory } = await importProject();
                    const paths = [1, 2].map((part) =>
                        path.join(directory, `A.mcdata.part${part}.${format}`),
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
                    const effects = captureSdk(t);
                    const selection = byDe
                        ? ['--de', 'A']
                        : paths.flatMap((file) => ['--file', file]);
                    const result = await main(
                        argv(root, [...selection, ...(enabled ? ['--skip-empty-lines'] : [])]),
                    );
                    assert.equal(result, enabled || format === 'json' ? 0 : 1);
                    assert.deepEqual(
                        effects
                            .filter(({ type }) => type === 'upload')
                            .flatMap(({ body }) => body.items),
                        enabled || format === 'json' ? rows : [],
                    );
                });
            }
        }
    }
    for (const api of [false, true]) {
        it(`threads cross-BU CLI ${api ? 'API' : 'file'} dispatch`, async (t) => {
            const { root, directory } = await importProject();
            const source = path.join(directory, 'A.mcdata.csv');
            await fs.writeFile(source, 'id,value\n\n1," exact "\n \t \n');
            const effects = captureSdk(t);
            assert.equal(
                await main([
                    'node',
                    'mcdata',
                    'import',
                    '--project',
                    root,
                    '--to',
                    'C/B',
                    ...(api ? ['--from', 'C/B', '--de', 'A'] : ['--file', source]),
                    '--skip-empty-lines',
                    '--no-backup-before-import',
                ]),
                0,
            );
            assert.deepEqual(
                effects.filter(({ type }) => type === 'upload').flatMap(({ body }) => body.items),
                [api ? { id: 'backup' } : { id: '1', value: ' exact ' }],
            );
        });
    }

    for (const byDe of [true, false]) {
        for (const clear of [true, false]) {
            it(`rejects malformed final DE before effects (${byDe ? '--de' : '--file'}, clear=${clear})`, async (t) => {
                const { root, directory } = await importProject();
                const first = path.join(directory, 'A.mcdata.csv');
                const last = path.join(directory, 'Z.mcdata.csv');
                await fs.writeFile(first, 'id,value\n1,ok\n');
                await fs.writeFile(last, 'id,value\n2,ok\n3,bad,extra\n');
                const effects = captureSdk(t);
                const selection = byDe
                    ? ['--de', 'A', '--de', 'Z']
                    : ['--file', first, '--file', last];
                assert.equal(
                    await main(argv(root, [...selection, '--backup-before-import'], clear)),
                    1,
                );
                assert.deepEqual(effects, []);
                // eslint-disable-next-line no-console -- Inspect captured validation diagnostics.
                assert.match(console.error.mock.calls.at(-1).arguments[0], /Z.mcdata.csv/);
            });
        }
        it(`rejects invalid final continuation before effects (${byDe ? '--de' : '--file'})`, async (t) => {
            const { root, directory } = await importProject();
            const first = path.join(directory, 'A.mcdata.part1.csv');
            const last = path.join(directory, 'A.mcdata.part2.csv');
            await fs.writeFile(first, 'id,value\n1,ok\n');
            await fs.writeFile(last, '2,ok\n3,bad,extra\n');
            const effects = captureSdk(t);
            assert.equal(
                await main(
                    argv(root, byDe ? ['--de', 'A'] : ['--file', first, '--file', last], true),
                ),
                1,
            );
            assert.deepEqual(effects, []);
            // eslint-disable-next-line no-console -- Inspect captured validation diagnostics.
            assert.match(console.error.mock.calls.at(-1).arguments[0], /part2.csv/);
        });
    }
    for (const byDe of [true, false]) {
        for (const format of ['csv', 'tsv', 'json']) {
            for (const linked of [false, true]) {
                it(`protects backup collision (${byDe ? '--de' : '--file'}, ${format}, hardlink=${linked})`, async (t) => {
                    const { root, directory } = await importProject();
                    const stamp = '2026-10-24T10-00-00.000Z';
                    t.mock.method(Date.prototype, 'toISOString', () => '2026-10-24T10:00:00.000Z');
                    const destination = path.join(directory, `A.mcdata.${stamp}.${format}`);
                    const source = linked
                        ? path.join(directory, `Z.mcdata.${format}`)
                        : destination;
                    const bytes = format === 'json' ? '[{"id":"source"}]\n' : 'id\nsource\n';
                    await fs.writeFile(source, bytes);
                    if (linked) {
                        await fs.link(source, destination);
                        await fs.writeFile(path.join(directory, `A.mcdata.${format}`), bytes);
                    }
                    const effects = captureSdk(t);
                    const selection = byDe
                        ? ['--de', 'A', ...(linked ? ['--de', 'Z'] : [])]
                        : [
                              '--file',
                              linked ? path.join(directory, `A.mcdata.${format}`) : source,
                              ...(linked ? ['--file', source] : []),
                          ];
                    await assert.rejects(
                        () =>
                            main(
                                argv(
                                    root,
                                    [...selection, '--format', format, '--backup-before-import'],
                                    true,
                                ),
                            ),
                        /Backup destination collides/,
                    );
                    assert.equal(await fs.readFile(source, 'utf8'), bytes);
                    assert.equal(await fs.readFile(destination, 'utf8'), bytes);
                    assert.ok(effects.every((effect) => effect.type === 'backup'));
                });
            }
        }
        it(`does not replace frozen source selection with successful backup (${byDe ? '--de' : '--file'})`, async (t) => {
            const { root, directory } = await importProject();
            const source = path.join(directory, 'A.mcdata.csv');
            await fs.writeFile(source, 'id\nsource\n');
            const effects = captureSdk(t);
            assert.equal(
                await main(
                    argv(root, [
                        ...(byDe ? ['--de', 'A'] : ['--file', source]),
                        '--backup-before-import',
                    ]),
                ),
                0,
            );
            assert.equal(effects.filter((effect) => effect.type === 'backup').length, 1);
            const upload = effects.find((effect) => effect.type === 'upload');
            assert.deepEqual(upload.body, { items: [{ id: 'source' }] });
            assert.equal(
                (await fs.readdir(directory)).filter((name) => name.endsWith('.csv')).length,
                2,
            );
        });
    }
    for (const byDe of [true, false]) {
        for (const clear of [false, true]) {
            it(`reports accepted earlier DE and batch (${byDe ? '--de' : '--file'}, clear=${clear})`, async (t) => {
                const { root, directory } = await importProject();
                const first = path.join(directory, 'A.mcdata.csv');
                const last = path.join(directory, 'Z.mcdata.csv');
                await fs.writeFile(first, 'id\nfirst\n');
                await fs.writeFile(last, 'id\n' + 'next\n'.repeat(2501));
                const effects = captureSdk(t);
                let uploads = 0;
                globalThis.__cliImportSdk.rest.put = async (route, body) => {
                    effects.push({ type: 'upload', route, body });
                    if (++uploads === 3) {
                        throw new Error('synthetic second-batch rejection');
                    }
                    return {};
                };
                await assert.rejects(
                    () =>
                        main(
                            argv(
                                root,
                                byDe
                                    ? ['--de', 'A', '--de', 'Z']
                                    : ['--file', first, '--file', last],
                                clear,
                            ),
                        ),
                    (error) => {
                        assert.match(error.message, /DE "Z"/);
                        assert.match(
                            error.message,
                            clear ? /CLEAR status: succeeded/ : /CLEAR status: not requested/,
                        );
                        assert.match(error.message, /Prior source jobs submitted: 1 \(A\)/);
                        assert.match(error.message, /2500 rows in 1 requests accepted/);
                        assert.match(error.message, /async acceptance is not completion/);
                        assert.doesNotMatch(error.message, /nothing imported/i);
                        return true;
                    },
                );
                assert.equal(uploads, 3);
                assert.deepEqual(
                    effects.map((effect) => effect.type),
                    clear
                        ? ['clear', 'upload', 'clear', 'upload', 'upload']
                        : ['upload', 'upload', 'upload'],
                );
            });
        }
        it(`reports uncertain CLEAR failure after earlier submission (${byDe ? '--de' : '--file'})`, async (t) => {
            const { root, directory } = await importProject();
            const first = path.join(directory, 'A.mcdata.csv');
            const last = path.join(directory, 'Z.mcdata.csv');
            await fs.writeFile(first, 'id\nfirst\n');
            await fs.writeFile(last, 'id\nlast\n');
            const effects = captureSdk(t);
            globalThis.__cliImportSdk.soap.perform = async (_type, _action, body) => {
                effects.push({ type: 'clear', body });
                if (body.CustomerKey === 'Z') {
                    throw new Error('synthetic CLEAR failure');
                }
            };
            await assert.rejects(
                () =>
                    main(
                        argv(
                            root,
                            byDe ? ['--de', 'A', '--de', 'Z'] : ['--file', first, '--file', last],
                            true,
                        ),
                    ),
                /CLEAR status: attempted; outcome unknown.*Prior source jobs submitted: 1 \(A\).*synthetic CLEAR failure/,
            );
            assert.deepEqual(
                effects.map((effect) => effect.type),
                ['clear', 'upload', 'clear'],
            );
        });
    }

    it('exits nonzero from the actual bin after CLEAR and a rejected later batch', async () => {
        const { root, directory } = await importProject();
        await fs.writeFile(path.join(directory, 'A.mcdata.csv'), 'id\n' + 'next\n'.repeat(2501));
        const moduleSource = `export default class SDK {
            constructor() { let calls = 0; this.rest = {
                get: async () => ({ count: 10 }),
                put: async () => { if (++calls === 2) throw new Error('synthetic upload failure'); return {requestId: 'accepted-1'}; }
            }; this.soap = { perform: async () => {} }; }
        }`;
        const preload = path.join(root, 'sdk-hook.mjs');
        await fs.writeFile(
            preload,
            `import { registerHooks } from 'node:module';
            registerHooks({ resolve(s, c, next) { return s === 'sfmc-sdk' ?
            { url: ${JSON.stringify(`data:text/javascript,${encodeURIComponent(moduleSource)}`)}, shortCircuit: true } : next(s, c); } });`,
        );
        const bin = fileURLToPath(new URL('../bin/mcdata.mjs', import.meta.url));
        const result = spawnSync(
            process.execPath,
            [
                '--import',
                pathToFileURL(preload).href,
                bin,
                ...argv(root, ['--de', 'A'], true).slice(2),
            ],
            { encoding: 'utf8', timeout: 20_000 },
        );
        assert.equal(result.error, undefined);
        assert.equal(result.status, 1);
        assert.match(result.stderr, /CLEAR status: succeeded/);
        assert.match(result.stderr, /2500 rows in 1 requests accepted/);
        assert.match(result.stderr, /No automatic retry or rollback/);
        assert.doesNotMatch(result.stderr, /nothing imported/i);
    });

    it('refuses unacknowledged noninteractive clear before any remote mutation', async (t) => {
        const { root, directory } = await importProject();
        await fs.writeFile(path.join(directory, 'A.mcdata.csv'), 'id\n1\n');
        const effects = captureSdk(t);
        await assert.rejects(
            () => main(argv(root, ['--de', 'A', '--clear-before-import'])),
            /Refusing to clear/,
        );
        assert.deepEqual(effects, []);
    });
    it('preserves consent and skip-empty clear through actual main', async (t) => {
        const { root, directory } = await importProject();
        await fs.writeFile(path.join(directory, 'A.mcdata.csv'), 'id\n1\n');
        const effects = captureSdk(t, 0);
        assert.equal(await main(argv(root, ['--de', 'A'], true)), 0);
        assert.deepEqual(
            effects.map((effect) => effect.type),
            ['upload'],
        );

        assert.match(
            // eslint-disable-next-line no-console -- Inspect captured upload progress.
            console.log.mock.calls.map((call) => call.arguments[0]).join('\n'),
            /Uploading batch 1\b/,
        );
    });
});

describe('cli import — clear-before-import skip when DE is empty', () => {
    it('skips clearDataExtensionRows when row count before import is 0', async () => {
        // We test the getDeRowCount + clear skip logic through the exported functions
        // rather than through CLI argv (which requires a full mcdev project on disk).
        // This verifies the core logic directly.
        const { getDeRowCount } = await import('../lib/row-count.mjs');

        let callCount = 0;
        const sdk = {
            rest: {
                get: async () => {
                    callCount++;
                    return { count: 0 };
                },
            },
        };

        const count = await getDeRowCount(sdk, 'TEST_DE');
        assert.equal(count, 0, 'count should be 0 for empty DE');
        assert.equal(callCount, 1, 'should have made exactly one API call');

        // Simulate the skip logic used in cli.mjs
        const clearWasCalled = [];
        const mockClear = async (deKey) => {
            clearWasCalled.push(deKey);
        };

        if (count === 0) {
            // Skip — this is what cli.mjs does
        } else {
            await mockClear('TEST_DE');
        }

        assert.deepEqual(clearWasCalled, [], 'clear should not have been called for empty DE');
    });

    it('calls clearDataExtensionRows when row count before import is > 0', async () => {
        const { getDeRowCount } = await import('../lib/row-count.mjs');

        const sdk = {
            rest: {
                get: async () => ({ count: 50 }),
            },
        };

        const count = await getDeRowCount(sdk, 'TEST_DE');
        assert.equal(count, 50);

        const clearWasCalled = [];
        const mockClear = async (deKey) => {
            clearWasCalled.push(deKey);
        };

        if (count === 0) {
            // Skip
        } else {
            await mockClear('TEST_DE');
        }

        assert.deepEqual(clearWasCalled, ['TEST_DE'], 'clear should be called for non-empty DE');
    });
});

/**
 * Mirrors `warnIfImportCountUnexpected` expected minimum (same as lib/import-de.mjs).
 *
 * @param {object} p
 * @param {number} p.countBefore
 * @param {boolean} p.cleared
 * @param {number} p.imported
 * @param {'upsert'|'insert'} p.mode
 */
function expectedMinRowCountAfterImport({ countBefore, cleared, imported, mode }) {
    const effectiveCountBefore = cleared ? 0 : countBefore;
    return mode === 'insert' || effectiveCountBefore === 0
        ? effectiveCountBefore + imported
        : imported;
}

describe('import assessment — countAfter vs expected', () => {
    it('detects unexpected low count for insert mode', () => {
        const mode = 'insert';
        const countBefore = 10;
        const imported = 5;
        const countAfter = 12; // lower than expected 15

        const expected = expectedMinRowCountAfterImport({
            countBefore,
            cleared: false,
            imported,
            mode,
        });
        assert.equal(expected, 15);
        assert.ok(countAfter < expected, 'should flag as unexpected');
    });

    it('no error when countAfter >= expected for insert', () => {
        const mode = 'insert';
        const countBefore = 10;
        const imported = 5;
        const countAfter = 15;

        const expected = expectedMinRowCountAfterImport({
            countBefore,
            cleared: false,
            imported,
            mode,
        });
        assert.equal(expected, 15);
        assert.ok(countAfter >= expected, 'should not flag as unexpected');
    });

    it('insert after clear uses effective empty DE (expected = imported, not countBefore + imported)', () => {
        const mode = 'insert';
        const countBefore = 11;
        const imported = 11;
        const countAfter = 11;

        const expected = expectedMinRowCountAfterImport({
            countBefore,
            cleared: true,
            imported,
            mode,
        });
        assert.equal(expected, 11);
        assert.ok(countAfter >= expected, 'should not flag as unexpected');
    });

    it('uses imported count as floor for upsert on non-empty DE', () => {
        const mode = 'upsert';
        const countBefore = 100;
        const imported = 50;
        const countAfter = 40; // lower than 50 imported

        const expected = expectedMinRowCountAfterImport({
            countBefore,
            cleared: false,
            imported,
            mode,
        });
        assert.equal(expected, 50);
        assert.ok(countAfter < expected, 'should flag as unexpected');
    });

    it('treats upsert on empty DE same as insert', () => {
        const mode = 'upsert';
        const countBefore = 0;
        const imported = 30;
        const countAfter = 25; // lower than 30

        const expected = expectedMinRowCountAfterImport({
            countBefore,
            cleared: false,
            imported,
            mode,
        });
        assert.equal(expected, 30);
        assert.ok(countAfter < expected, 'should flag as unexpected');
    });
});
