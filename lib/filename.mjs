/**
 * Mirrors sfmc-devtools `File.filterIllegalFilenames` / `reverseFilterIllegalFilenames`
 * so export filenames stay consistent with mcdev retrieve-style paths.
 * @see https://github.com/Accenture/sfmc-devtools (lib/util/file.js)
 */

/**
 * @param {string} filename
 * @returns {string}
 */
export function filterIllegalFilenames(filename) {
    return (
        encodeURIComponent(filename)
            .replaceAll(/[*]/g, '_STAR_')
            .split('%20')
            .join(' ')
            .split('%7B')
            .join('{')
            .split('%7D')
            .join('}')
            .split('%5B')
            .join('[')
            .split('%5D')
            .join(']')
            .split('%40')
            .join('@')
    );
}

/**
 * @param {string} filename
 * @returns {string}
 */
export function reverseFilterIllegalFilenames(filename) {
    return decodeURIComponent(filename).split('_STAR_').join('*');
}

/** Sentinel between encoded DE key and timestamp in export basenames; cannot appear in the key segment after encoding. */
export const MCDATA_SENTINEL = '+MCDATA+';

/**
 * @param {string} customerKey
 * @param {string} safeTs - filesystem-safe UTC timestamp
 * @param {'csv'|'tsv'|'json'} ext
 * @returns {string} basename without directory
 */
export function buildExportBasename(customerKey, safeTs, ext) {
    return `${filterIllegalFilenames(customerKey)}${MCDATA_SENTINEL}${safeTs}.${ext}`;
}

/**
 * @param {Date} [d]
 * @returns {string} e.g. 2026-04-06T15-48-30Z
 */
export function filesystemSafeTimestamp(d = new Date()) {
    return d.toISOString().replaceAll(':', '-');
}

/**
 * @param {string} basename - e.g. `encodedKey+MCDATA+2026-04-06T15-00-00.000Z.csv`
 * @returns {{ customerKey: string, timestampPart: string, ext: string }}
 */
export function parseExportBasename(basename) {
    const lastDot = basename.lastIndexOf('.');
    const stem = lastDot === -1 ? basename : basename.slice(0, lastDot);
    const ext = lastDot === -1 ? '' : basename.slice(lastDot + 1);
    const idx = stem.indexOf(MCDATA_SENTINEL);
    if (idx === -1) {
        throw new Error(
            `Filename must contain "${MCDATA_SENTINEL}" between encoded key and timestamp: ${basename}`
        );
    }
    const encodedKey = stem.slice(0, idx);
    const timestampPart = stem.slice(idx + MCDATA_SENTINEL.length);
    return {
        customerKey: reverseFilterIllegalFilenames(encodedKey),
        timestampPart,
        ext: ext.toLowerCase(),
    };
}
