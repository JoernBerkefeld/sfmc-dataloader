import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
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
        const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-de-list-'));
        try {
            await assert.rejects(() => fetchDeList(tmp, 'cred', 'bu'), /No project config found/);
        } finally {
            await fs.rm(tmp, { recursive: true, force: true });
        }
    });
});
