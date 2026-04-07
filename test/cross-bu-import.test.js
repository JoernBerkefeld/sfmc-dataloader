import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { crossBuImport } from '../lib/cross-bu-import.mjs';
import { buildExportBasename, parseExportBasename } from '../lib/filename.mjs';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs/promises';

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

describe('crossBuImport — API mode', () => {
    it('is exported as a function', () => {
        assert.strictEqual(typeof crossBuImport, 'function');
    });

    it('rejects when source credential is missing from mcdevrc', async () => {
        const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'cbi-test-'));
        await assert.rejects(
            () =>
                crossBuImport({
                    projectRoot: tmpDir,
                    mcdevrc,
                    mcdevAuth,
                    sourceCred: 'UnknownCred',
                    sourceBu: 'Dev',
                    targets: [{ credential: 'MyCred', bu: 'QA' }],
                    deKeys: ['DE1'],
                    format: 'csv',
                    api: 'async',
                    mode: 'upsert',
                    clearBeforeImport: false,
                    acceptRiskFlag: false,
                    isTTY: false,
                }),
            /UnknownCred/
        );
        await fs.rm(tmpDir, { recursive: true, force: true });
    });

    it('rejects when target BU is unknown', async () => {
        const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'cbi-test-'));
        await assert.rejects(
            () =>
                crossBuImport({
                    projectRoot: tmpDir,
                    mcdevrc,
                    mcdevAuth,
                    sourceCred: 'MyCred',
                    sourceBu: 'Dev',
                    targets: [{ credential: 'MyCred', bu: 'NonExistent' }],
                    deKeys: ['DE1'],
                    format: 'csv',
                    api: 'async',
                    mode: 'upsert',
                    clearBeforeImport: false,
                    acceptRiskFlag: false,
                    isTTY: false,
                }),
            /NonExistent/
        );
        await fs.rm(tmpDir, { recursive: true, force: true });
    });
});

describe('crossBuImport — file mode', () => {
    it('rejects when a target BU is unknown', async () => {
        const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'cbi-file-test-'));
        const fakeCsv = path.join(tmpDir, buildExportBasename('My_DE', '2026-04-08T10-00-00.000Z', 'csv'));
        await fs.writeFile(fakeCsv, 'col1,col2\nval1,val2\n', 'utf8');
        await assert.rejects(
            () =>
                crossBuImport({
                    projectRoot: tmpDir,
                    mcdevrc,
                    mcdevAuth,
                    filePaths: [fakeCsv],
                    targets: [{ credential: 'MyCred', bu: 'NonExistent' }],
                    format: 'csv',
                    api: 'async',
                    mode: 'upsert',
                    clearBeforeImport: false,
                    acceptRiskFlag: false,
                    isTTY: false,
                }),
            /NonExistent/
        );
        await fs.rm(tmpDir, { recursive: true, force: true });
    });

    it('derives DE key from +MCDATA+ filename correctly', () => {
        // Verify the filename parsing that crossBuImport uses in file mode.
        // Pure unit test — no network calls.
        const basename = buildExportBasename('Contact_DE', '2026-04-08T10-00-00.000Z', 'csv');
        const { customerKey } = parseExportBasename(basename);
        assert.equal(customerKey, 'Contact_DE');
    });
});
