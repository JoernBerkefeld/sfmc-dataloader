/**
 * Mirrors sfmc-devtools `File.filterIllegalFilenames` / `reverseFilterIllegalFilenames`
 * so export filenames stay consistent with mcdev retrieve-style paths.
 *
 * @see https://github.com/Accenture/sfmc-devtools (lib/util/file.js)
 */

/**
 * Literal segment between encoded DE key and timestamp (or extension in --git mode).
 */
export const MCDATA_SEGMENT = '.mcdata.';

/**
 * @param {string} filename
 * @returns {string}
 */
export function filterIllegalFilenames(filename) {
    return encodeURIComponent(filename)
        .replaceAll('*', '_STAR_')
        .replaceAll('%20', ' ')
        .replaceAll('%7B', '{')
        .replaceAll('%7D', '}')
        .replaceAll('%5B', '[')
        .replaceAll('%5D', ']')
        .replaceAll('%40', '@');
}

/**
 * @param {string} filename
 * @returns {string}
 */
export function reverseFilterIllegalFilenames(filename) {
    return decodeURIComponent(filename).replaceAll('_STAR_', '*');
}

/**
 * @param {string} customerKey
 * @param {string} safeTs - filesystem-safe UTC timestamp (ignored when isGitMode is true)
 * @param {'csv'|'tsv'|'json'} extension
 * @param {boolean} [isGitMode] - stable `key.mcdata.ext` without timestamp
 * @param {number} [partNumber] - when set, inserts `partN` before the timestamp (or before the extension in --git mode)
 * @returns {string} basename without directory
 */
export function buildExportBasename(customerKey, safeTs, extension, isGitMode = false, partNumber) {
    const enc = filterIllegalFilenames(customerKey);
    const partInfix =
        partNumber !== undefined && partNumber !== null ? `part${Number(partNumber)}.` : '';
    if (isGitMode) {
        if (partNumber !== undefined && partNumber !== null) {
            return `${enc}.mcdata.part${Number(partNumber)}.${extension}`;
        }
        return `${enc}.mcdata.${extension}`;
    }
    return `${enc}${MCDATA_SEGMENT}${partInfix}${safeTs}.${extension}`;
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
    const extension = lastDot === -1 ? '' : basename.slice(lastDot + 1).toLowerCase();

    const index = stem.indexOf(MCDATA_SEGMENT);
    if (index !== -1) {
        const encodedKey = stem.slice(0, index);
        let rest = stem.slice(index + MCDATA_SEGMENT.length);
        /**
         * @type {number|undefined}
         */
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
            ext: extension,
            ...(typeof partNumber === 'number' && { partNumber }),
        };
    }

    if (stem.endsWith('.mcdata')) {
        const encodedKey = stem.slice(0, -'.mcdata'.length);
        return {
            customerKey: reverseFilterIllegalFilenames(encodedKey),
            timestampPart: '',
            ext: extension,
        };
    }

    throw new Error(
        `Filename must contain ".mcdata." or end with ".mcdata" before the extension: ${basename}`,
    );
}
