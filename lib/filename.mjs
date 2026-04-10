/**
 * Mirrors sfmc-devtools `File.filterIllegalFilenames` / `reverseFilterIllegalFilenames`
 * so export filenames stay consistent with mcdev retrieve-style paths.
 *
 * @see https://github.com/Accenture/sfmc-devtools (lib/util/file.js)
 */

/** Literal segment between encoded DE key and timestamp (or extension in --git mode). */
export const MCDATA_SEGMENT = '.mcdata.';

/**
 * @param {string} filename
 * @returns {string}
 */
export function filterIllegalFilenames(filename) {
    return encodeURIComponent(filename)
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
        .join('@');
}

/**
 * @param {string} filename
 * @returns {string}
 */
export function reverseFilterIllegalFilenames(filename) {
    return decodeURIComponent(filename).split('_STAR_').join('*');
}

/**
 * @param {string} customerKey
 * @param {string} safeTs - filesystem-safe UTC timestamp (ignored when useGit is true)
 * @param {'csv'|'tsv'|'json'} ext
 * @param {boolean} [useGit] - stable `key.mcdata.ext` without timestamp
 * @returns {string} basename without directory
 */
export function buildExportBasename(customerKey, safeTs, ext, useGit = false) {
    const enc = filterIllegalFilenames(customerKey);
    if (useGit) {
        return `${enc}.mcdata.${ext}`;
    }
    return `${enc}${MCDATA_SEGMENT}${safeTs}.${ext}`;
}

/**
 * @param {Date} [d]
 * @returns {string} e.g. 2026-04-06T15-48-30Z
 */
export function filesystemSafeTimestamp(d = new Date()) {
    return d.toISOString().replaceAll(':', '-');
}

/**
 * @param {string} basename - e.g. `encodedKey.mcdata.2026-04-06T15-00-00.000Z.csv` or `encodedKey.mcdata.csv`
 * @returns {{ customerKey: string, timestampPart: string, ext: string }}
 */
export function parseExportBasename(basename) {
    const lastDot = basename.lastIndexOf('.');
    const stem = lastDot === -1 ? basename : basename.slice(0, lastDot);
    const ext = lastDot === -1 ? '' : basename.slice(lastDot + 1).toLowerCase();

    const idx = stem.indexOf(MCDATA_SEGMENT);
    if (idx !== -1) {
        const encodedKey = stem.slice(0, idx);
        const timestampPart = stem.slice(idx + MCDATA_SEGMENT.length);
        return {
            customerKey: reverseFilterIllegalFilenames(encodedKey),
            timestampPart,
            ext,
        };
    }

    if (stem.endsWith('.mcdata')) {
        const encodedKey = stem.slice(0, -'.mcdata'.length);
        return {
            customerKey: reverseFilterIllegalFilenames(encodedKey),
            timestampPart: '',
            ext,
        };
    }

    throw new Error(
        `Filename must contain ".mcdata." or end with ".mcdata" before the extension: ${basename}`,
    );
}
