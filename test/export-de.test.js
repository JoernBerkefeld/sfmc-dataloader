import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { fetchAllRowObjects, serializeRows } from '../lib/export-de.mjs';

describe('fetchAllRowObjects', () => {
    it('uses getBulk with page size 2500 and maps items to flat rows', async () => {
        const calls = [];
        const sdk = {
            rest: {
                getBulk: async (path, pageSize) => {
                    calls.push({ path, pageSize });
                    assert.ok(path.includes('rowset'));
                    assert.equal(pageSize, 2500);
                    return {
                        items: [
                            {
                                keys: { pk: '1' },
                                values: { a: 'x' },
                            },
                            {
                                keys: { pk: '2' },
                                values: { a: 'y' },
                            },
                        ],
                        count: 2,
                    };
                },
            },
        };
        const rows = await fetchAllRowObjects(sdk, 'DE1');
        assert.deepEqual(rows, [
            { pk: '1', a: 'x' },
            { pk: '2', a: 'y' },
        ]);
        assert.equal(calls.length, 1);
    });
});

describe('serializeRows', () => {
    it('writes CSV with BOM', () => {
        const s = serializeRows([{ a: '1' }], 'csv', false);
        assert.ok(s.startsWith('\uFEFF'));
    });
});
