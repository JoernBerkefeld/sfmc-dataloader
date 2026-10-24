import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { createFixtureRoot } from './fixture-root.js';
import { fetchDeList, normalizeDeListFromBulkResult } from '../lib/de-list.mjs';

describe('normalizeDeListFromBulkResult', () => {
    it('maps Results to sorted name/key items and drops rows without CustomerKey', () => {
        const bulk = {
            Results: [
                { Name: 'Zebra', CustomerKey: 'k_z' },
                { Name: 'Alpha', CustomerKey: 'k_a' },
                { Name: 'NoKey', CustomerKey: '' },
            ],
        };
        const items = normalizeDeListFromBulkResult(bulk);
        assert.deepEqual(items, [
            { name: 'Alpha', key: 'k_a' },
            { name: 'Zebra', key: 'k_z' },
        ]);
    });

    it('returns empty array when Results missing or empty', () => {
        assert.deepEqual(normalizeDeListFromBulkResult(), []);
        assert.deepEqual(normalizeDeListFromBulkResult({}), []);
        assert.deepEqual(normalizeDeListFromBulkResult({ Results: [] }), []);
    });

    it('coerces Name and CustomerKey to string', () => {
        const bulk = {
            Results: [{ Name: 1, CustomerKey: 2 }],
        };
        const items = normalizeDeListFromBulkResult(bulk);
        assert.deepEqual(items, [{ name: '1', key: '2' }]);
    });
});

describe('fetchDeList', () => {
    it('rejects when project root has no config', async () => {
        const temporary = await createFixtureRoot('mcdata-de-list-');
        try {
            await assert.rejects(
                () => fetchDeList(temporary, 'cred', 'bu'),
                /No project config found/,
            );
        } finally {
            // Retain the small, owned synthetic fixture for diagnostics.
        }
    });
});
