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
 * @param {number} [partNumber] - when set, inserts `partN` before the timestamp (or before the extension in --git mode)
 * @returns {string} basename without directory
 */
export function buildExportBasename(customerKey, safeTs, ext, useGit = false, partNumber) {
    const enc = filterIllegalFilenames(customerKey);
    const partInfix =
        partNumber !== undefined && partNumber !== null ? `part${Number(partNumber)}.` : '';
    if (useGit) {
        if (partNumber !== undefined && partNumber !== null) {
            return `${enc}.mcdata.part${Number(partNumber)}.${ext}`;
        }
        return `${enc}.mcdata.${ext}`;
    }
    return `${enc}${MCDATA_SEGMENT}${partInfix}${safeTs}.${ext}`;
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
 * @returns {{ customerKey: string, timestampPart: string, ext: string, partNumber?: number }}
 */
export function parseExportBasename(basename) {
    const lastDot = basename.lastIndexOf('.');
    const stem = lastDot === -1 ? basename : basename.slice(0, lastDot);
    const ext = lastDot === -1 ? '' : basename.slice(lastDot + 1).toLowerCase();

    const idx = stem.indexOf(MCDATA_SEGMENT);
    if (idx !== -1) {
        const encodedKey = stem.slice(0, idx);
        let rest = stem.slice(idx + MCDATA_SEGMENT.length);
        /** @type {number|undefined} */
        let partNumber;
        const partTs = rest.match(/^part(\d+)\.(.+)$/);
        if (partTs) {
            partNumber = Number(partTs[1]);
            rest = partTs[2];
        } else {
            const partOnly = rest.match(/^part(\d+)$/);
            if (partOnly) {
                partNumber = Number(partOnly[1]);
                rest = '';
            }
        }
        return {
            customerKey: reverseFilterIllegalFilenames(encodedKey),
            timestampPart: rest,
            ext,
            ...(typeof partNumber === 'number' ? { partNumber } : {}),
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
