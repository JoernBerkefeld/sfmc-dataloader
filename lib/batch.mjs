/** Default max UTF-8 bytes per request body (Data API family; margin below 5.9 MB). */
export const DEFAULT_MAX_BODY_BYTES = 5_500_000;

/** Max rows per HTTP payload chunk (byte cap in `DEFAULT_MAX_BODY_BYTES` may split further). */
export const MAX_OBJECTS_PER_BATCH = 2_500;

/**
 * Split rows into chunks that respect both max row count and serialized JSON body size.
 * Uses JSON.stringify on `{ items: chunk }` to estimate bytes (same shape as REST body).
 *
 * @param {object[]} rows - row objects (flat field map)
 * @param {object} [opts]
 * @param {number} [opts.maxBytes]
 * @param {number} [opts.maxObjects]
 * @returns {object[][]}
 */
export function chunkItemsForPayload(rows, opts = {}) {
    const maxBytes = opts.maxBytes ?? DEFAULT_MAX_BODY_BYTES;
    const maxObjects = opts.maxObjects ?? MAX_OBJECTS_PER_BATCH;
    const out = [];
    let i = 0;
    while (i < rows.length) {
        const chunk = [];
        while (i < rows.length && chunk.length < maxObjects) {
            const next = rows[i];
            const trial = [...chunk, next];
            const bytes = Buffer.byteLength(JSON.stringify({ items: trial }), 'utf8');
            if (bytes > maxBytes) {
                if (chunk.length > 0) {
                    break;
                }
                chunk.push(next);
                i++;
                break;
            }
            chunk.push(next);
            i++;
        }
        out.push(chunk);
    }
    return out;
}
