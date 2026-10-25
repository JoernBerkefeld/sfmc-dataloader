import { types } from 'node:util';

/**
 * Default max UTF-8 bytes per request body (Data API family; margin below 5.9 MB).
 */
export const DEFAULT_MAX_BODY_BYTES = 5_500_000;

/**
 * Max rows per HTTP payload chunk (byte cap in `DEFAULT_MAX_BODY_BYTES` may split further).
 */
export const MAX_OBJECTS_PER_BATCH = 2_500;

/**
 * Build a single chunk starting at `startIndex`, respecting both the max row
 * count and the serialized JSON body size budget.
 *
 * @param {object[]} rows - row objects (flat field map)
 * @param {number} startIndex - index of the first row to consider
 * @param {number} maxBytes - max serialized body size in bytes
 * @param {number} maxObjects - max number of rows per chunk
 * @returns {{ chunk: object[], nextIndex: number }}
 */
function buildChunk(rows, startIndex, maxBytes, maxObjects) {
    const chunk = [];
    let index = startIndex;
    while (index < rows.length && chunk.length < maxObjects) {
        const next = rows[index];
        const trial = [...chunk, next];
        const bytes = Buffer.byteLength(JSON.stringify({ items: trial }), 'utf8');
        if (bytes > maxBytes) {
            if (chunk.length > 0) {
                break;
            }
            chunk.push(next);
            index++;
            break;
        }
        chunk.push(next);
        index++;
    }
    return { chunk, nextIndex: index };
}

/**
 * Recognize inert JSON data without invoking getters, toJSON methods or proxy traps.
 * Anything uncertain keeps the original prefix-serialization behavior.
 *
 * @param {unknown} value
 * @param {WeakSet<object>} seen
 * @returns {boolean}
 */
function isPlainJsonData(value, seen) {
    if (value === null || ['string', 'number', 'boolean'].includes(typeof value)) {
        return true;
    }
    if (typeof value !== 'object' || types.isProxy(value) || seen.has(value)) {
        return false;
    }
    const prototype = Object.getPrototypeOf(value);
    if (![Object.prototype, Array.prototype, null].includes(prototype) || 'toJSON' in value) {
        return false;
    }
    if (Array.isArray(value)) {
        for (let index = 0; index < value.length; index++) {
            if (!Object.hasOwn(value, index)) {
                return false;
            }
        }
    }
    seen.add(value);
    return Object.values(Object.getOwnPropertyDescriptors(value)).every(
        (descriptor) => 'value' in descriptor && isPlainJsonData(descriptor.value, seen),
    );
}

/**
 * Split rows into chunks that respect both max row count and serialized JSON body size.
 * Plain JSON data uses additive UTF-8 sizing; custom serialization uses the original path.
 *
 * @param {object[]} rows - row objects (flat field map)
 * @param {object} [options]
 * @param {number} [options.maxBytes]
 * @param {number} [options.maxObjects]
 * @returns {object[][]}
 */
export function chunkItemsForPayload(rows, options = {}) {
    const maxBytes = options.maxBytes ?? DEFAULT_MAX_BODY_BYTES;
    const maxObjects = options.maxObjects ?? MAX_OBJECTS_PER_BATCH;
    const out = [];
    if (
        typeof maxBytes === 'number' &&
        typeof maxObjects === 'number' &&
        Array.isArray(rows) &&
        isPlainJsonData(rows, new WeakSet())
    ) {
        const wrapperBytes = Buffer.byteLength(JSON.stringify({ items: [] }), 'utf8');
        let chunk = [];
        let bytes = wrapperBytes;
        // eslint-disable-next-line unicorn/no-for-loop -- Match the original indexed access without invoking a custom iterator.
        for (let index = 0; index < rows.length; index++) {
            const row = rows[index];
            const rowBytes = Buffer.byteLength(JSON.stringify(row), 'utf8');
            if (
                chunk.length > 0 &&
                (chunk.length >= maxObjects || bytes + 1 + rowBytes > maxBytes)
            ) {
                out.push(chunk);
                chunk = [];
                bytes = wrapperBytes;
            }
            bytes += (chunk.length > 0 ? 1 : 0) + rowBytes;
            chunk.push(row);
            // Match the original immediate flush for a single oversized row.
            if (!(bytes > maxBytes || chunk.length >= maxObjects)) {
                continue;
            }

            out.push(chunk);
            chunk = [];
            bytes = wrapperBytes;
        }
        if (chunk.length > 0) {
            out.push(chunk);
        }
        return out;
    }
    let index = 0;
    while (index < rows.length) {
        const { chunk, nextIndex } = buildChunk(rows, index, maxBytes, maxObjects);
        index = nextIndex;
        out.push(chunk);
    }
    return out;
}
