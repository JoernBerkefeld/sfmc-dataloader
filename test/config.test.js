import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
    loadMcdevProject,
    parseCredBu,
    resolveCredentialAndMid,
    buildSdkAuthObject,
    buildSdkOptions,
} from '../lib/config.mjs';

describe('config', () => {
    let tmp;
    before(async () => {
        tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        await fs.writeFile(
            path.join(tmp, '.mcdevrc.json'),
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
            path.join(tmp, '.mcdev-auth.json'),
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
        await fs.rm(tmp, { recursive: true, force: true });
    });

    it('loadMcdevProject reads JSON', () => {
        const { mcdevrc, mcdevAuth } = loadMcdevProject(tmp);
        assert.equal(mcdevrc.credentials.R1.businessUnits.DEV, 123456);
        assert.equal(mcdevAuth.R1.client_id, 'id');
    });

    it('parseCredBu splits credential and BU', () => {
        assert.deepEqual(parseCredBu('R1/DEV'), { credential: 'R1', bu: 'DEV' });
    });

    it('resolveCredentialAndMid builds MID', () => {
        const { mcdevrc, mcdevAuth } = loadMcdevProject(tmp);
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

describe('buildSdkOptions', () => {
    it('returns basic options when logger is null', () => {
        const options = buildSdkOptions(null);
        assert.equal(options.requestAttempts, 3);
        assert.equal(options.eventHandlers, undefined);
    });

    it('returns basic options when logger is not provided', () => {
        const options = buildSdkOptions();
        assert.equal(options.requestAttempts, 3);
        assert.equal(options.eventHandlers, undefined);
    });

    it('includes eventHandlers when logger is provided', () => {
        const logs = [];
        const mockLogger = { write: (text) => logs.push(text), logPath: '/tmp/test.log' };
        const options = buildSdkOptions(mockLogger);
        assert.equal(options.requestAttempts, 3);
        assert.ok(options.eventHandlers, 'eventHandlers should be defined');
        assert.equal(typeof options.eventHandlers.logRequest, 'function');
        assert.equal(typeof options.eventHandlers.logResponse, 'function');
    });

    it('logRequest writes to logger', () => {
        const logs = [];
        const mockLogger = { write: (text) => logs.push(text), logPath: '/tmp/test.log' };
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
        const mockLogger = { write: (text) => logs.push(text), logPath: '/tmp/test.log' };
        const options = buildSdkOptions(mockLogger);
        options.eventHandlers.logResponse({ status: 200, data: { result: 'ok' } });
        assert.ok(logs.some((l) => l.includes('API RESPONSE << 200')));
        assert.ok(logs.some((l) => l.includes('RESPONSE BODY <<')));
    });

    it('logResponse formats body with newline and 2-space indent', () => {
        const logs = [];
        const mockLogger = { write: (text) => logs.push(text), logPath: '/tmp/test.log' };
        const options = buildSdkOptions(mockLogger);
        options.eventHandlers.logResponse({ status: 200, data: { foo: 'bar', baz: 123 } });
        const bodyLog = logs.find((l) => l.startsWith('RESPONSE BODY <<'));
        assert.ok(bodyLog, 'should have RESPONSE BODY log');
        assert.ok(bodyLog.startsWith('RESPONSE BODY <<\n'), 'body should start with newline');
        assert.ok(bodyLog.includes('  {'), 'body lines should be indented with 2 spaces');
    });
});
