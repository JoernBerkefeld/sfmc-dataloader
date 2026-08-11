import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import fs from 'node:fs/promises';
import fss from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { normalizeBuName, processBusinessUnitResults } from '../lib/business-units.mjs';
import { runMcdataInit } from '../lib/init-project.mjs';

describe('normalizeBuName', () => {
    it('leaves plain names unchanged', () => {
        assert.equal(normalizeBuName('DEV'), 'DEV');
    });

    it('replaces spaces with underscores', () => {
        assert.equal(normalizeBuName('My BU'), 'My_BU');
    });

    it('collapses multiple spaces to a single underscore', () => {
        assert.equal(normalizeBuName('My  BU'), 'My_BU');
    });

    it('strips special characters', () => {
        assert.equal(normalizeBuName('My-BU (prod)'), 'MyBU_prod');
    });

    it('collapses consecutive underscores after stripping special chars', () => {
        assert.equal(normalizeBuName('A--B'), 'AB');
    });
});

describe('processBusinessUnitResults', () => {
    const ROWS = [
        { ID: '100', ParentID: '0', Name: 'Parent', IsActive: 'true' },
        { ID: '200', ParentID: '100', Name: 'Dev BU', IsActive: 'true' },
        { ID: '300', ParentID: '100', Name: 'Prod-BU (main)', IsActive: 'true' },
    ];

    it('returns eid from parent row', () => {
        const { eid } = processBusinessUnitResults(ROWS, 999);
        assert.equal(eid, 100);
    });

    it('stores parent BU under _ParentBU_ key', () => {
        const { businessUnits } = processBusinessUnitResults(ROWS, 999);
        assert.equal(businessUnits['_ParentBU_'], 100);
    });

    it('normalizes child BU names', () => {
        const { businessUnits } = processBusinessUnitResults(ROWS, 999);
        assert.equal(businessUnits['Dev_BU'], 200);
        assert.equal(businessUnits['ProdBU_main'], 300);
    });

    it('sorts child BU keys alphabetically with _ParentBU_ first', () => {
        const rows = [
            { ID: '100', ParentID: '0', Name: 'Parent', IsActive: 'true' },
            { ID: '300', ParentID: '100', Name: 'Zebra BU', IsActive: 'true' },
            { ID: '200', ParentID: '100', Name: 'Alpha BU', IsActive: 'true' },
        ];
        const { businessUnits } = processBusinessUnitResults(rows, 999);
        assert.deepEqual(Object.keys(businessUnits), ['_ParentBU_', 'Alpha_BU', 'Zebra_BU']);
        assert.equal(businessUnits['Alpha_BU'], 200);
        assert.equal(businessUnits['Zebra_BU'], 300);
    });

    it('falls back to provided enterpriseId when no parent row present', () => {
        const rows = [{ ID: '200', ParentID: '100', Name: 'Child', IsActive: 'true' }];
        const { eid } = processBusinessUnitResults(rows, 42);
        assert.equal(eid, 42);
    });

    it('handles empty results without throwing', () => {
        const { eid, businessUnits } = processBusinessUnitResults([], 55);
        assert.equal(eid, 55);
        assert.deepEqual(businessUnits, {});
    });
});

/**
 * Create a stub fetchBusinessUnits that returns a fixed result.
 * Used by patching the import inside init-project.mjs is not needed —
 * instead we call runMcdataInit with a non-interactive path where isTTY is false
 * and verify file output.
 *
 * We pass a custom buFetcher override via dependency injection.
 * To avoid module mocking, we add an optional `_buFetcher` param to runMcdataInit.
 * The production code uses the real fetchBusinessUnits from business-units.mjs.
 */

describe('runMcdataInit', () => {
    const FIXED_BU_RESULT = {
        eid: 100,
        businessUnits: { _ParentBU_: 100, Dev: 200 },
    };

    async function initWithFakeBu(directory, extraOptions = {}) {
        return runMcdataInit({
            projectRoot: directory,
            isTTY: false,
            credential: 'TestOrg',
            clientId: 'cid',
            clientSecret: 'csec',
            authUrl: 'https://mc.auth.marketingcloudapis.com/',
            enterpriseId: '100',
            yes: true,
            stdout: () => {},
            stderr: () => {},
            _buFetcher: async () => FIXED_BU_RESULT,
            ...extraOptions,
        });
    }

    it('returns 1 when both .mcdevrc.json and .mcdev-auth.json exist', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            await fs.writeFile(path.join(directory, '.mcdevrc.json'), '{}');
            await fs.writeFile(path.join(directory, '.mcdev-auth.json'), '{}');
            const errors = [];
            const code = await runMcdataInit({
                projectRoot: directory,
                isTTY: false,
                yes: true,
                stderr: (message) => {
                    errors.push(message);
                },
                stdout: () => {},
                _buFetcher: async () => FIXED_BU_RESULT,
            });
            assert.equal(code, 1);
            assert.ok(
                errors.some((m) => m.includes('.mcdevrc.json') && m.includes('.mcdev-auth.json')),
            );
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('proceeds when only .mcdevrc.json exists without .mcdev-auth.json', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            await fs.writeFile(path.join(directory, '.mcdevrc.json'), '{}');
            const code = await initWithFakeBu(directory);
            assert.equal(code, 0);
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('returns 1 when .mcdatarc.json exists in non-interactive mode without --yes', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            await fs.writeFile(path.join(directory, '.mcdatarc.json'), '{}');
            const errors = [];
            const code = await runMcdataInit({
                projectRoot: directory,
                isTTY: false,
                credential: 'TestOrg',
                clientId: 'cid',
                clientSecret: 'csec',
                authUrl: 'https://mc.auth.marketingcloudapis.com/',
                enterpriseId: '100',
                yes: false,
                stderr: (message) => {
                    errors.push(message);
                },
                stdout: () => {},
                _buFetcher: async () => FIXED_BU_RESULT,
            });
            assert.equal(code, 1);
            assert.ok(errors.some((m) => m.includes('Pass --yes')));
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('skips overwrite confirmation when --yes is true and .mcdatarc.json exists', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            await fs.writeFile(path.join(directory, '.mcdatarc.json'), '{}');
            const code = await initWithFakeBu(directory);
            assert.equal(code, 0);
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('calls _confirm and aborts when user answers no', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            await fs.writeFile(path.join(directory, '.mcdatarc.json'), '{}');
            const messages = [];
            const code = await runMcdataInit({
                projectRoot: directory,
                isTTY: true,
                credential: 'TestOrg',
                clientId: 'cid',
                clientSecret: 'csec',
                authUrl: 'https://mc.auth.marketingcloudapis.com/',
                enterpriseId: '100',
                yes: false,
                stdout: (message) => {
                    messages.push(message);
                },
                stderr: () => {},
                _confirm: async () => false,
                _buFetcher: async () => FIXED_BU_RESULT,
            });
            assert.equal(code, 1);
            assert.ok(messages.some((m) => m.includes('Aborted')));
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('calls _confirm and proceeds when user answers yes', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            await fs.writeFile(path.join(directory, '.mcdatarc.json'), '{}');
            const code = await runMcdataInit({
                projectRoot: directory,
                isTTY: true,
                credential: 'TestOrg',
                clientId: 'cid',
                clientSecret: 'csec',
                authUrl: 'https://mc.auth.marketingcloudapis.com/',
                enterpriseId: '100',
                yes: false,
                stdout: () => {},
                stderr: () => {},
                _confirm: async () => true,
                _buFetcher: async () => FIXED_BU_RESULT,
            });
            assert.equal(code, 0);
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('returns 1 in non-interactive mode when required flags are missing', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            const errors = [];
            const code = await runMcdataInit({
                projectRoot: directory,
                isTTY: false,
                stdout: () => {},
                stderr: (message) => {
                    errors.push(message);
                },
            });
            assert.equal(code, 1);
            assert.ok(errors.some((m) => m.includes('missing required flags')));
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('writes .mcdatarc.json and .mcdata-auth.json on success', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            const code = await initWithFakeBu(directory);
            assert.equal(code, 0);
            const rc = JSON.parse(fss.readFileSync(path.join(directory, '.mcdatarc.json'), 'utf8'));
            assert.equal(rc.credentials.TestOrg.eid, 100);
            assert.equal(rc.credentials.TestOrg.businessUnits.Dev, 200);
            const auth = JSON.parse(
                fss.readFileSync(path.join(directory, '.mcdata-auth.json'), 'utf8'),
            );
            assert.equal(auth.TestOrg.client_id, 'cid');
            assert.equal(auth.TestOrg.account_id, 100);
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('creates .gitignore with .mcdata-auth.json entry', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            await initWithFakeBu(directory);
            const gitignore = fss.readFileSync(path.join(directory, '.gitignore'), 'utf8');
            assert.ok(gitignore.includes('.mcdata-auth.json'));
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });

    it('appends .mcdata-auth.json to existing .gitignore without duplication', async () => {
        const directory = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-'));
        try {
            await fs.writeFile(path.join(directory, '.gitignore'), 'node_modules\n');
            await initWithFakeBu(directory);
            const gitignore = fss.readFileSync(path.join(directory, '.gitignore'), 'utf8');
            assert.ok(gitignore.includes('node_modules'));
            assert.ok(gitignore.includes('.mcdata-auth.json'));
            const count = (gitignore.match(/\.mcdata-auth\.json/g) ?? []).length;
            assert.equal(count, 1);
            // Run again — still only one entry
            await initWithFakeBu(directory);
            const gitignore2 = fss.readFileSync(path.join(directory, '.gitignore'), 'utf8');
            const count2 = (gitignore2.match(/\.mcdata-auth\.json/g) ?? []).length;
            assert.equal(count2, 1);
        } finally {
            await fs.rm(directory, { recursive: true, force: true });
        }
    });
});
