import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { multiBuExport } from '../lib/multi-bu-export.mjs';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs/promises';

const mcdevrc = {
    credentials: {
        MyCred: {
            businessUnits: {
                Dev: 111,
                QA: 222,
            },
        },
    },
};
const mcdevAuth = {
    MyCred: {
        client_id: 'cid',
        client_secret: 'csec',
        auth_url: 'https://mc00000000000000000000000000.auth.marketingcloudapis.com/',
    },
};

describe('multiBuExport', () => {
    it('accepts an empty sources array without throwing and returns []', async () => {
        const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'mbe-test-empty-'));
        const result = await multiBuExport({
            projectRoot: tmpDir,
            mcdevrc,
            mcdevAuth,
            sources: [],
            deKeys: ['DE1'],
            format: 'csv',
        });
        assert.deepEqual(result, []);
        await fs.rm(tmpDir, { recursive: true, force: true });
    });

    it('rejects when a source credential is missing from mcdevrc', async () => {
        const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'mbe-test-missing-'));
        await assert.rejects(
            () =>
                multiBuExport({
                    projectRoot: tmpDir,
                    mcdevrc,
                    mcdevAuth,
                    sources: [{ credential: 'UnknownCred', bu: 'Dev' }],
                    deKeys: ['DE1'],
                    format: 'csv',
                }),
            /UnknownCred/,
        );
        await fs.rm(tmpDir, { recursive: true, force: true });
    });

    it('rejects when a source BU is missing from mcdevrc', async () => {
        const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), 'mbe-test-missing-bu-'));
        await assert.rejects(
            () =>
                multiBuExport({
                    projectRoot: tmpDir,
                    mcdevrc,
                    mcdevAuth,
                    sources: [{ credential: 'MyCred', bu: 'NonExistent' }],
                    deKeys: ['DE1'],
                    format: 'csv',
                }),
            /NonExistent/,
        );
        await fs.rm(tmpDir, { recursive: true, force: true });
    });
});
