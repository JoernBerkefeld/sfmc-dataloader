import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    chunkItemsForPayload,
    MAX_OBJECTS_PER_BATCH,
    DEFAULT_MAX_BODY_BYTES,
} from '../lib/batch.mjs';

describe('chunkItemsForPayload', () => {
    it('respects max object count', () => {
        const rows = Array.from({ length: MAX_OBJECTS_PER_BATCH + 100 }, (_, i) => ({ id: i }));
        const chunks = chunkItemsForPayload(rows);
        assert.ok(chunks.every((c) => c.length <= MAX_OBJECTS_PER_BATCH));
        const total = chunks.reduce((s, c) => s + c.length, 0);
        assert.equal(total, rows.length);
    });

    it('splits when payload would exceed byte budget', () => {
        const big = 'x'.repeat(1_000_000);
        const rows = [{ a: big }, { b: big }];
        const chunks = chunkItemsForPayload(rows, { maxBytes: 500_000, maxObjects: 100 });
        assert.ok(chunks.length >= 2);
        for (const ch of chunks) {
            const bytes = Buffer.byteLength(JSON.stringify({ items: ch }), 'utf8');
            assert.ok(bytes <= DEFAULT_MAX_BODY_BYTES || ch.length === 1);
        }
    });

    it('allows a single oversized row in its own chunk', () => {
        const huge = { x: 'y'.repeat(10_000_000) };
        const chunks = chunkItemsForPayload([huge], { maxBytes: 1000, maxObjects: 10 });
        assert.equal(chunks.length, 1);
        assert.equal(chunks[0].length, 1);
    });
});
