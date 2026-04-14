import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { importRowsForDe } from '../lib/import-de.mjs';

describe('importRowsForDe', () => {
    it('calls put for upsert in chunks', async () => {
        const calls = [];
        const sdk = {
            rest: {
                put: async (p, body) => {
                    calls.push({ path: p, n: body.items.length });
                    return {};
                },
                post: async () => {
                    throw new Error('unexpected post');
                },
            },
        };
        const rows = Array.from({ length: 3 }, (_, i) => ({ id: String(i) }));
        const result = await importRowsForDe(sdk, {
            deKey: 'K',
            rows,
            mode: 'upsert',
        });
        assert.equal(result.count, 3);
        assert.deepEqual(result.requestIds, [null]);
        assert.equal(calls.length, 1);
        assert.equal(calls[0].n, 3);
        assert.ok(calls[0].path.includes('async'));
    });

    it('calls post for insert', async () => {
        const sdk = {
            rest: {
                put: async () => {
                    throw new Error('unexpected put');
                },
                post: async () => ({ requestId: 'req-123' }),
            },
        };
        const result = await importRowsForDe(sdk, {
            deKey: 'K',
            rows: [{ a: 1 }],
            mode: 'insert',
        });
        assert.equal(result.count, 1);
        assert.deepEqual(result.requestIds, ['req-123']);
    });
});
