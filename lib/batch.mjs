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
 * Split rows into chunks that respect both max row count and serialized JSON body size.
 * Uses JSON.stringify on `{ items: chunk }` to estimate bytes (same shape as REST body).
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
    let index = 0;
    while (index < rows.length) {
        const { chunk, nextIndex } = buildChunk(rows, index, maxBytes, maxObjects);
        index = nextIndex;
        out.push(chunk);
    }
    return out;
}
