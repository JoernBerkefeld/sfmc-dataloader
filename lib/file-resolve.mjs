import fs from 'node:fs/promises';
import path from 'node:path';
import { parseExportBasename } from './filename.mjs';

/**
 * @typedef {{ path: string, partNumber: number|null, mtime: number }} CandidateEntry
 */

/**
 * Group multi-part exports (`part1`, `part2`, … sharing the same key and timestamp) and pick
 * the newest export run. Returns ordered paths (part 1, 2, …) or a single file path.
 *
 * @param {string[]} candidatePaths - from {@link findImportCandidates}
 * @returns {Promise.<{ paths: string[], isMultiPart: boolean }>}
 */
export async function resolveImportSet(candidatePaths) {
    /** @type {Map<string, CandidateEntry[]>} */
    const groups = new Map();
    for (const filePath of candidatePaths) {
        const name = path.basename(filePath);
        let parsed;
        try {
            parsed = parseExportBasename(name);
        } catch {
            continue;
        }
        const { customerKey, timestampPart, ext, partNumber } = parsed;
        const groupKey = `${customerKey}\0${timestampPart}\0${ext}`;
        let st;
        try {
            st = await fs.stat(filePath);
        } catch {
            continue;
        }
        const entry = {
            path: filePath,
            partNumber: partNumber === undefined ? null : partNumber,
            mtime: st.mtimeMs,
        };
        const list = groups.get(groupKey);
        if (list) {
            list.push(entry);
        } else {
            groups.set(groupKey, [entry]);
        }
    }
    if (groups.size === 0) {
        return { paths: [], isMultiPart: false };
    }
    let bestEntries = /** @type {CandidateEntry[]|null} */ (null);
    let bestMaxMtime = -1;
    for (const entries of groups.values()) {
        const maxM = Math.max(...entries.map((e) => e.mtime));
        if (maxM > bestMaxMtime) {
            bestMaxMtime = maxM;
            bestEntries = entries;
        }
    }
    if (!bestEntries || bestEntries.length === 0) {
        return { paths: [], isMultiPart: false };
    }
    const hasParts = bestEntries.some((e) => e.partNumber !== null);
    if (hasParts) {
        const sorted = [...bestEntries].toSorted(
            (a, b) => (a.partNumber ?? 0) - (b.partNumber ?? 0),
        );
        return {
            paths: sorted.map((e) => e.path),
            isMultiPart: sorted.length > 1,
        };
    }
    const one = bestEntries.reduce((a, b) => (a.mtime >= b.mtime ? a : b));
    return { paths: [one.path], isMultiPart: false };
}

/** Supported import/export file extensions */
const SUPPORTED_EXTENSIONS = ['csv', 'tsv', 'json'];

/**
 * Derive format from file extension.
 *
 * @param {string} filePath
 * @returns {'csv'|'tsv'|'json'|null}
 */
export function formatFromExtension(filePath) {
    const ext = path.extname(filePath).toLowerCase().slice(1);
    if (ext === 'csv') {
        return 'csv';
    }
    if (ext === 'tsv') {
        return 'tsv';
    }
    if (ext === 'json') {
        return 'json';
    }
    return null;
}

/**
 * Find export files under data dir matching the DE customer key and extension.
 * When format is omitted, searches all supported extensions (csv, tsv, json).
 *
 * @param {string} dataDir
 * @param {string} customerKey
 * @param {'csv'|'tsv'|'json'} [format] - optional; if omitted, searches all extensions
 * @returns {Promise.<string[]>} full paths
 */
export async function findImportCandidates(dataDir, customerKey, format) {
    let entries;
    try {
        entries = await fs.readdir(dataDir, { withFileTypes: true });
    } catch {
        return [];
    }
    const extensions = format ? [format] : SUPPORTED_EXTENSIONS;
    const matches = [];
    for (const ent of entries) {
        if (!ent.isFile()) {
            continue;
        }
        const name = ent.name;
        const fileExt = path.extname(name).toLowerCase().slice(1);
        if (!extensions.includes(fileExt)) {
            continue;
        }
        try {
            const { customerKey: parsedKey } = parseExportBasename(name);
            if (parsedKey === customerKey) {
                matches.push(path.join(dataDir, name));
            }
        } catch {
            continue;
        }
    }
    return matches;
}

/**
 * @param {string[]} paths
 * @returns {Promise.<string>} path with newest mtime
 */
export async function pickLatestByMtime(paths) {
    if (paths.length === 0) {
        throw new Error('No candidate files');
    }
    let best = paths[0];
    let bestTime = 0;
    for (const p of paths) {
        const st = await fs.stat(p);
        const t = st.mtimeMs;
        if (t >= bestTime) {
            bestTime = t;
            best = p;
        }
    }
    return best;
}
