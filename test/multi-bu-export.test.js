import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { multiBuExport } from '../lib/multi-bu-export.mjs';
import { createFixtureRoot } from './fixture-root.js';
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
        const temporaryDirectory = await createFixtureRoot('mbe-test-empty-');
        const result = await multiBuExport({
            projectRoot: temporaryDirectory,
            mcdevrc,
            mcdevAuth,
            sources: [],
            deKeys: ['DE1'],
            format: 'csv',
        });
        assert.deepEqual(result, []);
        // Retain the small, owned synthetic fixture for diagnostics.
    });

    it('rejects when a source credential is missing from mcdevrc', async () => {
        const temporaryDirectory = await createFixtureRoot('mbe-test-missing-');
        await assert.rejects(
            () =>
                multiBuExport({
                    projectRoot: temporaryDirectory,
                    mcdevrc,
                    mcdevAuth,
                    sources: [{ credential: 'UnknownCred', bu: 'Dev' }],
                    deKeys: ['DE1'],
                    format: 'csv',
                }),
            /UnknownCred/,
        );
        // Retain the small, owned synthetic fixture for diagnostics.
    });

    it('rejects when a source BU is missing from mcdevrc', async () => {
        const temporaryDirectory = await createFixtureRoot('mbe-test-missing-bu-');
        await assert.rejects(
            () =>
                multiBuExport({
                    projectRoot: temporaryDirectory,
                    mcdevrc,
                    mcdevAuth,
                    sources: [{ credential: 'MyCred', bu: 'NonExistent' }],
                    deKeys: ['DE1'],
                    format: 'csv',
                }),
            /NonExistent/,
        );
        // Retain the small, owned synthetic fixture for diagnostics.
    });
});
