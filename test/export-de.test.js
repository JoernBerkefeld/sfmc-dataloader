import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { fetchAllRowObjects, serializeRows } from '../lib/export-de.mjs';

describe('fetchAllRowObjects', () => {
    it('paginates until hasMoreRows is false', async () => {
        let page = 0;
        const sdk = {
            rest: {
                get: async (urlPath) => {
                    assert.ok(urlPath.includes('rowset'));
                    page++;
                    if (page === 1) {
                        return {
                            items: [
                                {
                                    keys: { pk: '1' },
                                    values: { a: 'x' },
                                },
                            ],
                            hasMoreRows: true,
                        };
                    }
                    return {
                        items: [
                            {
                                keys: { pk: '2' },
                                values: { a: 'y' },
                            },
                        ],
                        hasMoreRows: false,
                    };
                },
            },
        };
        const rows = await fetchAllRowObjects(sdk, 'DE1');
        assert.deepEqual(rows, [
            { pk: '1', a: 'x' },
            { pk: '2', a: 'y' },
        ]);
    });
});

describe('serializeRows', () => {
    it('writes CSV with BOM', () => {
        const s = serializeRows([{ a: '1' }], 'csv', false);
        assert.ok(s.startsWith('\ufeff'));
    });
});
