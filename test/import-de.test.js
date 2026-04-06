import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { importRowsForDe } from '../lib/import-de.mjs';

describe('importRowsForDe', () => {
    it('calls put for async upsert in chunks', async () => {
        const calls = [];
        const sdk = {
            rest: {
                put: async (path, body) => {
                    calls.push({ path, n: body.items.length });
                    return {};
                },
                post: async () => {
                    throw new Error('unexpected post');
                },
            },
        };
        const rows = Array.from({ length: 3 }, (_, i) => ({ id: String(i) }));
        await importRowsForDe(sdk, {
            deKey: 'K',
            rows,
            api: 'async',
            mode: 'upsert',
        });
        assert.equal(calls.length, 1);
        assert.equal(calls[0].n, 3);
        assert.ok(calls[0].path.includes('async'));
    });
});
