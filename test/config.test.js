import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
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
        temporary = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
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

    after(async () => {
        await fs.rm(temporary, { recursive: true, force: true });
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
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
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
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('warns when mcdev pair wins and mcdata files exist', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
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
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('throws on incomplete mcdev pair (rc only)', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            await fs.writeFile(path.join(directory, '.mcdevrc.json'), '{}');
            assert.throws(() => loadProjectConfig(directory), /Missing.*mcdev-auth/);
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('throws on incomplete mcdata pair (rc only)', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            await fs.writeFile(path.join(directory, '.mcdatarc.json'), '{}');
            assert.throws(() => loadProjectConfig(directory), /Missing.*mcdata-auth/);
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('throws when no config present', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            assert.throws(() => loadProjectConfig(directory), /No project config found/);
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });
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
