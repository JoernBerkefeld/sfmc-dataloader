import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { chunkItemsForPayload, MAX_OBJECTS_PER_BATCH } from '../lib/batch.mjs';

// Reference the original prefix algorithm, including its repeated serialization calls.
function original(rows, { maxBytes = 5_500_000, maxObjects = 2500 } = {}) {
    const chunks = [];
    let index = 0;
    while (index < rows.length) {
        const chunk = [];
        while (index < rows.length && chunk.length < maxObjects) {
            const next = rows[index];
            const bytes = Buffer.byteLength(JSON.stringify({ items: [...chunk, next] }));
            if (bytes > maxBytes && chunk.length > 0) {
                // eslint-disable-next-line unicorn/no-break-in-nested-loop -- Mirrors the original compatibility algorithm exactly.
                break;
            }
            chunk.push(next);
            index++;
            if (bytes > maxBytes) {
                // eslint-disable-next-line unicorn/no-break-in-nested-loop -- Mirrors the original compatibility algorithm exactly.
                break;
            }
        }
        chunks.push(chunk);
    }
    return chunks;
}

describe('chunkItemsForPayload', () => {
    it('serializes plain rows once each rather than growing prefixes', (t) => {
        const stringify = JSON.stringify;
        let rowVisits = 0;
        t.mock.method(JSON, 'stringify', (value, ...arguments_) => {
            rowVisits += value.items ? value.items.length : 1;
            return stringify(value, ...arguments_);
        });
        const rows = Array.from({ length: 2500 }, (_, id) => ({ id }));
        assert.equal(chunkItemsForPayload(rows)[0].length, rows.length);
        assert.equal(rowVisits, rows.length);
    });
    it('matches exact UTF-8 boundaries, order and original references', () => {
        const rows = Array.from({ length: 15 }, (_, id) => ({
            id,
            value: '😀漢é\\n\\"\\\\',
            nested: [null, true, { x: id }],
        }));
        const boundary = Buffer.byteLength(JSON.stringify({ items: rows.slice(0, 3) }));
        for (const maxBytes of [0, boundary - 1, boundary, boundary + 1, 10000]) {
            for (const maxObjects of [1, 3, 100]) {
                const options = { maxBytes, maxObjects };
                const actual = chunkItemsForPayload(rows, options);
                assert.deepEqual(actual, original(rows, options));
                assert.deepEqual(
                    actual.map((chunk) => JSON.stringify({ items: chunk })),
                    original(rows, options).map((chunk) => JSON.stringify({ items: chunk })),
                );
                for (const [index, row] of actual.flat().entries()) {
                    assert.equal(row, rows[index]);
                }
            }
        }
        assert.deepEqual(chunkItemsForPayload([]), []);
    });

    it('preserves toJSON keys, getter calls and proxy serialization side effects', () => {
        for (const kind of ['toJSON', 'getter', 'proxy']) {
            const run = (chunker) => {
                const calls = [];
                const rows = Array.from({ length: 4 }, (_, id) => {
                    if (kind === 'toJSON') {
                        return {
                            toJSON(key) {
                                calls.push([id, key]);
                                return { value: key };
                            },
                        };
                    }
                    if (kind === 'getter') {
                        return {
                            get value() {
                                calls.push(id);
                                return calls.length;
                            },
                        };
                    }
                    return new Proxy(
                        { id },
                        {
                            get(target, key) {
                                calls.push(String(key));
                                return Reflect.get(target, key);
                            },
                        },
                    );
                });
                const chunks = chunker(rows, { maxBytes: 40 });
                return {
                    calls,
                    chunks: chunks.map((chunk) => chunk.map((row) => rows.indexOf(row))),
                };
            };
            assert.deepEqual(run(chunkItemsForPayload), run(original));
        }
    });

    it('retains unsupported values, holes, custom prototypes and serialization errors', () => {
        const sparse = [1];
        sparse.length = 3;
        sparse[2] = 3;
        const rows = [
            undefined,
            () => 1,
            Symbol('x'),
            NaN,
            Infinity,
            { x: undefined },
            sparse,
            new Date(0),
            Object.assign(Object.create({ inherited: 1 }), { a: 1 }),
        ];
        assert.deepEqual(
            chunkItemsForPayload(rows, { maxBytes: 35 }),
            original(rows, { maxBytes: 35 }),
        );
        const cyclic = {};
        cyclic.self = cyclic;
        for (const row of [1n, cyclic]) {
            assert.throws(() => chunkItemsForPayload([row]), TypeError);
            assert.throws(() => original([row]), TypeError);
        }
    });

    it('respects max object count', () => {
        const rows = Array.from({ length: MAX_OBJECTS_PER_BATCH + 100 }, (_, index) => ({
            id: index,
        }));
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
            assert.ok(bytes <= 500_000 || ch.length === 1);
        }
    });

    it('allows a single oversized row in its own chunk', () => {
        const huge = { x: 'y'.repeat(10_000_000) };
        const chunks = chunkItemsForPayload([huge], { maxBytes: 1000, maxObjects: 10 });
        assert.equal(chunks.length, 1);
        assert.equal(chunks[0].length, 1);
    });
});
