import assert from 'node:assert/strict';
import { describe, it, before } from 'node:test';
import fs from 'node:fs/promises';
import fsSync from 'node:fs';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { loadPublicProjectConfig } from '../lib/index.mjs';
import { createFixtureRoot } from './fixture-root.js';
import path from 'node:path';
import SDK from 'sfmc-sdk';
import { axiosInstance, RestError } from 'sfmc-sdk/util';
import {
    loadMcdevProject,
    loadProjectConfig,
    WARN_MCDATA_SUPERSEDED,
    parseCredBu,
    resolveCredentialAndMid,
    buildSdkAuthObject,
    buildSdkOptions,
} from '../lib/config.mjs';

const readProjectFile = (file) => fs.readFile(new URL(`../${file}`, import.meta.url), 'utf8');

describe('Node runtime support', () => {
    it('keeps the manifest, lockfile, README, and CI aligned with dependency requirements', async () => {
        const manifest = JSON.parse(await readProjectFile('package.json'));
        const lockfile = JSON.parse(await readProjectFile('package-lock.json'));
        const nodeRange = '^22.22.2 || >=24.15.0';
        assert.equal(manifest.engines.node, nodeRange);
        assert.equal(lockfile.packages[''].engines.node, nodeRange);
        assert.equal(lockfile.packages['node_modules/sfmc-sdk'].engines.node, nodeRange);
        assert.equal(lockfile.packages['node_modules/mcdev'].engines.node, nodeRange);
        assert.ok((await readProjectFile('README.md')).includes(`- Node.js \`${nodeRange}\``));
        const workflow = await readProjectFile('.github/workflows/code-test.yml');
        assert.match(workflow, /node: \['22\.22\.2', '24\.15\.0', 25\]/u);
    });
});

describe('config', () => {
    let temporary;
    before(async () => {
        temporary = await createFixtureRoot('mcdata-');
        await fs.writeFile(
            path.join(temporary, '.mcdevrc.json'),
            JSON.stringify({
                credentials: {
                    R1: {
                        businessUnits: {
                            DEV: 123456,
                        },
                    },
                },
            }),
        );
        await fs.writeFile(
            path.join(temporary, '.mcdev-auth.json'),
            JSON.stringify({
                R1: {
                    client_id: 'id',
                    client_secret: 'secret',
                    auth_url: 'https://mcabcdefghijklmnop12345678.auth.marketingcloudapis.com/',
                },
            }),
        );
    });

    it('loadMcdevProject reads JSON', () => {
        const { mcdevrc, mcdevAuth } = loadMcdevProject(temporary);
        assert.equal(mcdevrc.credentials.R1.businessUnits.DEV, 123456);
        assert.equal(mcdevAuth.R1.client_id, 'id');
    });

    it('parseCredBu splits credential and BU', () => {
        assert.deepEqual(parseCredBu('R1/DEV'), { credential: 'R1', bu: 'DEV' });
    });

    it('resolveCredentialAndMid builds MID', () => {
        const { mcdevrc, mcdevAuth } = loadMcdevProject(temporary);
        const r = resolveCredentialAndMid(mcdevrc, mcdevAuth, 'R1', 'DEV');
        assert.equal(r.mid, 123456);
        const auth = buildSdkAuthObject(r.authCred, r.mid);
        assert.equal(auth.account_id, 123456);
        assert.ok(auth.auth_url.includes('auth.marketingcloudapis.com'));
    });

    it('parseCredBu rejects invalid', () => {
        assert.throws(() => parseCredBu('nope'), /credential/);
    });
});

describe('loadProjectConfig', () => {
    it('loads mcdata-only pair', async () => {
        const directory = await createFixtureRoot('mcdata-');
        try {
            await fs.writeFile(
                path.join(directory, '.mcdatarc.json'),
                JSON.stringify({
                    credentials: {
                        R1: { eid: 1, businessUnits: { _ParentBU_: 1, DEV: 2 } },
                    },
                }),
            );
            await fs.writeFile(
                path.join(directory, '.mcdata-auth.json'),
                JSON.stringify({
                    R1: {
                        client_id: 'id',
                        client_secret: 'secret',
                        auth_url: 'https://mcabcdefghijklmnop12345678.auth.marketingcloudapis.com/',
                    },
                }),
            );
            const { mcdevrc, mcdevAuth } = loadProjectConfig(directory, { stderr: () => {} });
            assert.equal(mcdevrc.credentials.R1.businessUnits.DEV, 2);
            assert.equal(mcdevAuth.R1.client_id, 'id');
        } finally {
            // Retain the small, owned synthetic fixture for diagnostics.
        }
    });

    it('warns when mcdev pair wins and mcdata files exist', async () => {
        const directory = await createFixtureRoot('mcdata-');
        try {
            await fs.writeFile(
                path.join(directory, '.mcdevrc.json'),
                JSON.stringify({
                    credentials: { R1: { businessUnits: { DEV: 123456 } } },
                }),
            );
            await fs.writeFile(
                path.join(directory, '.mcdev-auth.json'),
                JSON.stringify({
                    R1: {
                        client_id: 'id',
                        client_secret: 'secret',
                        auth_url: 'https://mcabcdefghijklmnop12345678.auth.marketingcloudapis.com/',
                    },
                }),
            );
            await fs.writeFile(path.join(directory, '.mcdatarc.json'), '{}');
            const warnings = [];
            loadProjectConfig(directory, {
                stderr: (message) => {
                    warnings.push(message);
                },
            });
            assert.ok(warnings.includes(WARN_MCDATA_SUPERSEDED));
        } finally {
            // Retain the small, owned synthetic fixture for diagnostics.
        }
    });

    it('throws on incomplete mcdev pair (rc only)', async () => {
        const directory = await createFixtureRoot('mcdata-');
        try {
            await fs.writeFile(path.join(directory, '.mcdevrc.json'), '{}');
            assert.throws(() => loadProjectConfig(directory), /Missing.*mcdev-auth/);
        } finally {
            // Retain the small, owned synthetic fixture for diagnostics.
        }
    });

    it('throws on incomplete mcdata pair (rc only)', async () => {
        const directory = await createFixtureRoot('mcdata-');
        try {
            await fs.writeFile(path.join(directory, '.mcdatarc.json'), '{}');
            assert.throws(() => loadProjectConfig(directory), /Missing.*mcdata-auth/);
        } finally {
            // Retain the small, owned synthetic fixture for diagnostics.
        }
    });

    it('throws when no config present', async () => {
        const directory = await createFixtureRoot('mcdata-');
        try {
            assert.throws(() => loadProjectConfig(directory), /No project config found/);
        } finally {
            // Retain the small, owned synthetic fixture for diagnostics.
        }
    });
});

describe('public config and safe parse errors', () => {
    const names = ['.mcdevrc.json', '.mcdev-auth.json', '.mcdatarc.json', '.mcdata-auth.json'];

    for (let mask = 0; mask < 16; mask++) {
        it(`matches synchronous pair selection for file presence mask ${mask}`, async () => {
            const root = await createFixtureRoot('mcdata-public-');
            for (const [index, name] of names.entries()) {
                if (mask & (1 << index)) {
                    await fs.writeFile(path.join(root, name), JSON.stringify({ source: name }));
                }
            }
            const syncWarnings = [];
            const asyncWarnings = [];
            let expected;
            let failure;
            try {
                expected = loadProjectConfig(root, {
                    stderr: (text) => {
                        syncWarnings.push(text);
                    },
                });
            } catch (ex) {
                failure = ex;
            }
            const result = loadPublicProjectConfig(root, {
                stderr: (text) => {
                    asyncWarnings.push(text);
                },
            });
            assert.ok(result instanceof Promise);
            if (failure) {
                await assert.rejects(result, { message: failure.message });
            } else {
                assert.deepEqual(await result, { mcdevrc: expected.mcdevrc });
            }
            assert.deepEqual(asyncWarnings, syncWarnings);
        });
    }

    it('never reads auth contents or uses synchronous filesystem APIs', async (t) => {
        const root = await createFixtureRoot('mcdata-public-');
        await fs.writeFile(path.join(root, '.mcdatarc.json'), '{"credentials":{}}');
        await fs.writeFile(path.join(root, '.mcdata-auth.json'), 'SECRET_SENTINEL invalid JSON');
        const readFile = fs.readFile.bind(fs);
        const reads = [];
        t.mock.method(fs, 'readFile', async (file, ...arguments_) => {
            reads.push(path.basename(file));
            return readFile(file, ...arguments_);
        });
        for (const method of ['existsSync', 'readFileSync']) {
            t.mock.method(fsSync, method, () => {
                assert.fail(`Unexpected synchronous filesystem operation: ${method}`);
            });
        }
        assert.deepEqual(await loadPublicProjectConfig(root), { mcdevrc: { credentials: {} } });
        assert.deepEqual(reads, ['.mcdatarc.json']);
    });

    for (const name of names) {
        it(`sanitizes malformed ${name} at the reader and CLI boundaries`, async () => {
            const root = await createFixtureRoot('mcdata-safe-json-');
            const pair = name.startsWith('.mcdev') ? names.slice(0, 2) : names.slice(2);
            for (const file of pair) {
                await fs.writeFile(path.join(root, file), '{}');
            }
            const sentinel = 'SYNTHETIC_SECRET_DO_NOT_DISCLOSE';
            await fs.writeFile(path.join(root, name), `{"client_secret":"${sentinel}", invalid}`);
            const check = (error) => {
                assert.ok(error instanceof SyntaxError);
                assert.equal(error.message, `Invalid JSON in ${name}. Fix the file and try again.`);
                assert.equal(error.cause, undefined);
                assert.ok(!String(error.stack).includes(sentinel));
                assert.ok(!JSON.stringify(error).includes(sentinel));
                return true;
            };
            assert.throws(() => loadProjectConfig(root), check);
            assert.throws(() => loadMcdevProject(root), check);
            if (name.endsWith('rc.json')) {
                await assert.rejects(loadPublicProjectConfig(root), check);
            } else {
                assert.deepEqual(await loadPublicProjectConfig(root), { mcdevrc: {} });
            }
            const cli = spawnSync(
                process.execPath,
                [
                    fileURLToPath(new URL('../bin/mcdata.mjs', import.meta.url)),
                    'export',
                    'C/B',
                    '--de',
                    'SyntheticDE',
                ],
                { cwd: root, encoding: 'utf8' },
            );
            assert.equal(cli.error, undefined);
            assert.notEqual(cli.status, 0);
            const output = cli.stdout + cli.stderr;
            assert.ok(output.includes(`Invalid JSON in ${name}`), output);
            assert.ok(!output.includes(sentinel), output);
        });
    }
});

describe('SDK authentication compatibility', () => {
    it('accepts newly introduced scopes and preserves token requests', async (t) => {
        const originalAdapter = axiosInstance.defaults.adapter;
        t.after(() => {
            axiosInstance.defaults.adapter = originalAdapter;
        });
        const scopes = ['data_extensions_read', 'future_sfmc_scope_read'];
        let tokenRequests = 0;
        axiosInstance.defaults.adapter = async (config) => {
            tokenRequests++;
            assert.equal(config.url, '/v2/token');
            assert.deepEqual(JSON.parse(config.data), {
                grant_type: 'client_credentials',
                client_id: 'id',
                client_secret: 'secret',
                account_id: 123456,
                scope: scopes.join(' '),
            });
            return {
                status: 200,
                statusText: 'OK',
                headers: {},
                config,
                data: {
                    access_token: 'test-token',
                    expires_in: 1200,
                    scope: scopes.join(' '),
                },
            };
        };
        const auth = buildSdkAuthObject(
            {
                client_id: 'id',
                client_secret: 'secret',
                auth_url: 'https://mcabcdefghijklmnopqrstuvwx12.auth.marketingcloudapis.com/',
            },
            123456,
        );
        const sdk = new SDK({ ...auth, scope: scopes }, buildSdkOptions());
        assert.ok(!sdk.auth.getSupportedScopes().includes(scopes[1]));
        const token = await sdk.auth.getAccessToken();
        assert.equal(token.access_token, 'test-token');
        assert.equal(token.scope, scopes.join(' '));
        assert.equal(tokenRequests, 1);
    });

    it('continues to surface genuine token endpoint failures', async (t) => {
        const originalAdapter = axiosInstance.defaults.adapter;
        t.after(() => {
            axiosInstance.defaults.adapter = originalAdapter;
        });
        axiosInstance.defaults.adapter = async () => {
            throw Object.assign(new Error('Unauthorized'), {
                response: {
                    status: 401,
                    data: {
                        error: 'invalid_client',
                        error_description: 'Invalid test credentials',
                    },
                },
            });
        };
        const sdk = new SDK(
            buildSdkAuthObject(
                {
                    client_id: 'id',
                    client_secret: 'secret',
                    auth_url: 'https://mcabcdefghijklmnopqrstuvwx12.auth.marketingcloudapis.com/',
                },
                123456,
            ),
            buildSdkOptions(),
        );
        await assert.rejects(sdk.auth.getAccessToken(), (error) => {
            assert.ok(error instanceof RestError);
            assert.equal(error.code, 'invalid_client');
            assert.equal(error.message, 'Invalid test credentials');
            return true;
        });
    });
});

describe('buildSdkOptions', () => {
    it('returns basic options when logger is null', () => {
        const options = buildSdkOptions(null);
        assert.equal(options.requestAttempts, 3);
        assert.equal(options.retryOnConnectionError, true);
        assert.ok(options.eventHandlers, 'eventHandlers should be defined');
        assert.equal(typeof options.eventHandlers.onLoop, 'function');
        assert.equal(typeof options.eventHandlers.onConnectionError, 'function');
    });

    it('returns basic options when logger is not provided', () => {
        const options = buildSdkOptions();
        assert.equal(options.requestAttempts, 3);
        assert.equal(options.retryOnConnectionError, true);
        assert.ok(options.eventHandlers, 'eventHandlers should be defined');
        assert.equal(typeof options.eventHandlers.onLoop, 'function');
        assert.equal(typeof options.eventHandlers.onConnectionError, 'function');
    });

    it('includes eventHandlers when logger is provided', () => {
        const logs = [];
        const mockLogger = {
            write: (text) => {
                logs.push(text);
            },
            logPath: '/tmp/test.log',
        };
        const options = buildSdkOptions(mockLogger);
        assert.equal(options.requestAttempts, 3);
        assert.equal(options.retryOnConnectionError, true);
        assert.ok(options.eventHandlers, 'eventHandlers should be defined');
        assert.equal(typeof options.eventHandlers.onLoop, 'function');
        assert.equal(typeof options.eventHandlers.onConnectionError, 'function');
        assert.equal(typeof options.eventHandlers.logRequest, 'function');
        assert.equal(typeof options.eventHandlers.logResponse, 'function');
    });

    it('logRequest writes to logger', () => {
        const logs = [];
        const mockLogger = {
            write: (text) => {
                logs.push(text);
            },
            logPath: '/tmp/test.log',
        };
        const options = buildSdkOptions(mockLogger);
        options.eventHandlers.logRequest({
            method: 'POST',
            url: '/data/v1/test',
            data: { foo: 'bar' },
        });
        assert.ok(logs.some((l) => l.includes('API REQUEST >> POST /data/v1/test')));
        assert.ok(logs.some((l) => l.includes('REQUEST BODY >>')));
    });

    for (const token of ['synthetic-compact-token', '\nsynthetic-multiline\ntoken\n']) {
        it(`redacts ${token.includes('\n') ? 'multiline' : 'compact'} SOAP authentication without changing the request`, () => {
            const logs = [];
            const { logRequest } = buildSdkOptions({
                write: (text) => {
                    logs.push(text);
                },
                logPath: 'unused.log',
            }).eventHandlers;
            const data =
                '<Envelope><Header><fueloauth xmlns="http://exacttarget.com">' +
                token +
                '</fueloauth><fueloauth>synthetic-second-token</fueloauth></Header>' +
                '<Body><Value>business-record</Value><fueloauthSuffix>keep</fueloauthSuffix></Body></Envelope>';
            const request = {
                method: 'post',
                url: '/Service.asmx',
                headers: { Authorization: 'Bearer synthetic-header-token' },
                data,
            };
            const original = structuredClone(request);
            logRequest(request);
            assert.deepEqual(request, original);
            assert.deepEqual(logs, [
                'API REQUEST >> POST /Service.asmx',
                'REQUEST BODY >> <Envelope><Header><fueloauth xmlns="http://exacttarget.com">*** TOKEN REMOVED ***</fueloauth>' +
                    '<fueloauth>*** TOKEN REMOVED ***</fueloauth></Header>' +
                    '<Body><Value>business-record</Value><fueloauthSuffix>keep</fueloauthSuffix></Body></Envelope>',
            ]);
            assert.ok(!logs.join('\n').includes(token));
            assert.ok(!logs.join('\n').includes('synthetic-second-token'));
            assert.ok(!logs.join('\n').includes('synthetic-header-token'));
        });
    }

    for (const header of ['Authorization', 'authorization', 'AUTHORIZATION']) {
        it(`keeps REST ${header} out of diagnostics and preserves business fields`, () => {
            const logs = [];
            const { logRequest } = buildSdkOptions({
                write: (text) => {
                    logs.push(text);
                },
                logPath: 'unused.log',
            }).eventHandlers;
            const request = {
                method: 'post',
                url: '/data/v1/async/dataextensions/key:Orders/rows',
                headers: { [header]: 'Bearer synthetic-rest-token' },
                data: { client_secret: 'business-value', rows: [{ token: 'business-record' }] },
            };
            const original = structuredClone(request);
            logRequest(request);
            assert.deepEqual(request, original);
            assert.deepEqual(logs, [
                `API REQUEST >> POST ${request.url}`,
                `REQUEST BODY >> ${JSON.stringify(request.data, null, 2)}`,
            ]);
            assert.ok(!logs.join('\n').includes('synthetic-rest-token'));
        });
    }

    it('limits XML masking to the exact SOAP endpoint', () => {
        const logs = [];
        const { logRequest } = buildSdkOptions({
            write: (text) => {
                logs.push(text);
            },
            logPath: 'unused.log',
        }).eventHandlers;
        const data = '<fueloauth>business-value</fueloauth>';
        logRequest({ url: '/other/Service.asmx', data });
        assert.deepEqual(logs, [
            'API REQUEST >> GET /other/Service.asmx',
            `REQUEST BODY >> ${data}`,
        ]);
    });

    it('preserves string and object response representations including authentication-like fields', () => {
        const logs = [];
        const { logResponse } = buildSdkOptions({
            write: (text) => {
                logs.push(text);
            },
            logPath: 'unused.log',
        }).eventHandlers;
        const responses = [
            { status: 200, data: '<fueloauth>response-value</fueloauth>\n<Rows>records</Rows>' },
            {
                statusCode: 202,
                data: { access_token: 'response-value', rows: [{ secret: 'record' }] },
            },
        ];
        const original = structuredClone(responses);
        for (const response of responses) {
            logResponse(response);
        }
        assert.deepEqual(responses, original);
        assert.deepEqual(logs, [
            'API RESPONSE << 200',
            'RESPONSE BODY <<\n  <fueloauth>response-value</fueloauth>\n  <Rows>records</Rows>',
            'API RESPONSE << 202',
            `RESPONSE BODY <<\n${JSON.stringify(responses[1].data, null, 2)
                .split('\n')
                .map((line) => '  ' + line)
                .join('\n')}`,
        ]);
    });

    it('logResponse writes to logger', () => {
        const logs = [];
        const mockLogger = {
            write: (text) => {
                logs.push(text);
            },
            logPath: '/tmp/test.log',
        };
        const options = buildSdkOptions(mockLogger);
        options.eventHandlers.logResponse({ status: 200, data: { result: 'ok' } });
        assert.ok(logs.some((l) => l.includes('API RESPONSE << 200')));
        assert.ok(logs.some((l) => l.includes('RESPONSE BODY <<')));
    });

    it('logResponse formats body with newline and 2-space indent', () => {
        const logs = [];
        const mockLogger = {
            write: (text) => {
                logs.push(text);
            },
            logPath: '/tmp/test.log',
        };
        const options = buildSdkOptions(mockLogger);
        options.eventHandlers.logResponse({ status: 200, data: { foo: 'bar', baz: 123 } });
        const bodyLog = logs.find((l) => l.startsWith('RESPONSE BODY <<'));
        assert.ok(bodyLog, 'should have RESPONSE BODY log');
        assert.ok(bodyLog.startsWith('RESPONSE BODY <<\n'), 'body should start with newline');
        assert.ok(bodyLog.includes('  {'), 'body lines should be indented with 2 spaces');
    });

    it('onLoop writes batch progress when context is provided', () => {
        const options = buildSdkOptions(null);
        const chunks = [];
        const orig = process.stdout.write.bind(process.stdout);
        process.stdout.write = (s) => {
            chunks.push(s);
            return true;
        };
        try {
            options.eventHandlers.onLoop(undefined, [1, 2], {
                nextPage: 2,
                totalPages: 4,
                accumulatedCount: 2500,
            });
        } finally {
            process.stdout.write = orig;
        }
        const out = chunks.join('');
        assert.ok(out.includes('Downloading batch 2 of 4'));
        assert.ok(out.includes('2500'));
    });

    it('onLoop writes legacy line when context is absent', () => {
        const options = buildSdkOptions(null);
        const chunks = [];
        const orig = process.stdout.write.bind(process.stdout);
        process.stdout.write = (s) => {
            chunks.push(s);
            return true;
        };
        try {
            options.eventHandlers.onLoop(undefined, [1, 2, 3]);
        } finally {
            process.stdout.write = orig;
        }
        const out = chunks.join('');
        assert.ok(out.includes('Downloading next batch'));
        assert.ok(out.includes('3'));
    });
});
