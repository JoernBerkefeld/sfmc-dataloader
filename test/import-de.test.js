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
        const n = await importRowsForDe(sdk, {
            deKey: 'K',
            rows,
            mode: 'upsert',
        });
        assert.equal(n, 3);
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
                post: async () => ({}),
            },
        };
        const n = await importRowsForDe(sdk, {
            deKey: 'K',
            rows: [{ a: 1 }],
            mode: 'insert',
        });
        assert.equal(n, 1);
    });
});
