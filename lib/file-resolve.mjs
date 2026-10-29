import fs from 'node:fs/promises';
import path from 'node:path';
import { parseExportBasename, resolveExplicitImportBasename } from './filename.mjs';

/**
 * @typedef {{ path: string, partNumber: number|null, mtime: number, statError?: Error }} CandidateEntry
 */

/**
 * Group multi-part exports (`part1`, `part2`, … sharing the same key and timestamp) and pick
 * the newest export run. Explicit selection requires exactly one run/format for the DE.
 * Returns ordered paths (part 1, 2, …) or a single file path.
 *
 * @param {string[]} candidatePaths - candidates for one DE from discovery or explicit selection
 * @param {object} [options]
 * @param {boolean} [options.explicitSelection] - reject multiple runs/formats instead of picking newest
 * @returns {Promise.<{ paths: string[], isMultiPart: boolean }>}
 */
export async function resolveImportSet(candidatePaths, { explicitSelection = false } = {}) {
    /**
     * @type {Map<string, CandidateEntry[]>}
     */
    const groups = new Map();
    for (const filePath of candidatePaths) {
        const name = path.basename(filePath);
        let parsed;
        try {
            parsed = explicitSelection
                ? resolveExplicitImportBasename(name)
                : parseExportBasename(name);
        } catch (ex) {
            if (explicitSelection) {
                throw ex;
            }
            continue;
        }
        const { customerKey, timestampPart, ext, partNumber } = parsed;
        const groupKey = `${customerKey}\0${timestampPart}\0${ext}`;
        /**
        @type {CandidateEntry}
         */
        const entry = {
            path: filePath,
            partNumber: partNumber === undefined ? null : partNumber,
            mtime: -Infinity,
        };
        try {
            const st = await fs.stat(filePath);
            entry.mtime = st.mtimeMs;
        } catch (ex) {
            // Keep recognizable members: a selected run must not silently lose a part.
            entry.statError = ex;
        }
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
    if (explicitSelection && groups.size > 1) {
        throw new Error(
            'Ambiguous explicit import selection: select only one export run and format per DE.',
        );
    }
    let bestEntries = /** @type {CandidateEntry[]|null} */ (null);
    let bestMaxMtime = -Infinity;
    for (const entries of groups.values()) {
        const maxM = Math.max(...entries.map((entry) => entry.mtime));
        if (!(maxM > bestMaxMtime)) {
            continue;
        }

        bestMaxMtime = maxM;
        bestEntries = entries;
    }
    if (!bestEntries) {
        const failed = groups.values().next().value[0];
        throw new Error(`Cannot stat import candidate: ${failed.path}`, {
            cause: failed.statError,
        });
    }
    const failed = bestEntries.find((entry) => entry.statError);
    if (failed) {
        throw new Error(`Cannot stat selected import file: ${failed.path}`, {
            cause: failed.statError,
        });
    }
    const hasParts = bestEntries.some((entry) => entry.partNumber !== null);
    const sorted = [...bestEntries].toSorted((a, b) => (a.partNumber ?? 0) - (b.partNumber ?? 0));
    if (hasParts) {
        if (sorted.some((entry) => entry.partNumber === null)) {
            throw new Error(
                `Selected import set mixes numbered and unnumbered files: ${sorted[0].path}`,
            );
        }
        const seen = new Set();
        for (const [index, entry] of sorted.entries()) {
            if (seen.has(entry.partNumber)) {
                throw new Error(
                    `Duplicate part ${entry.partNumber} in selected import set: ${entry.path}`,
                );
            }
            seen.add(entry.partNumber);
            if (entry.partNumber !== index + 1) {
                throw new Error(
                    `Selected import set requires contiguous parts starting at 1; expected part ${index + 1}: ${entry.path}`,
                );
            }
        }
    } else if (sorted.length !== 1) {
        throw new Error(`Duplicate unnumbered files in selected import set: ${sorted[0].path}`);
    }
    for (const entry of sorted) {
        let handle;
        try {
            handle = await fs.open(entry.path, 'r');
            if (!(await handle.stat()).isFile()) {
                throw new Error('Input is not a regular file');
            }
        } catch (ex) {
            throw new Error(`Cannot read selected import file: ${entry.path}`, { cause: ex });
        } finally {
            await handle?.close();
        }
    }
    return { paths: sorted.map((entry) => entry.path), isMultiPart: sorted.length > 1 };
}

/**
 * Supported import/export file extensions
 */
const SUPPORTED_EXTENSIONS = ['csv', 'tsv', 'json'];

/**
 * Derive format from file extension.
 *
 * @param {string} filePath
 * @returns {'csv'|'tsv'|'json'|null}
 */
export function formatFromExtension(filePath) {
    const extension = path.extname(filePath).toLowerCase().slice(1);
    if (extension === 'csv') {
        return 'csv';
    }
    if (extension === 'tsv') {
        return 'tsv';
    }
    if (extension === 'json') {
        return 'json';
    }
    return null;
}

/**
 * Find export files under data dir matching the DE customer key and extension.
 * When format is omitted, searches all supported extensions (csv, tsv, json).
 *
 * @param {string} dataDirectory
 * @param {string} customerKey
 * @param {'csv'|'tsv'|'json'} [format] - optional; if omitted, searches all extensions
 * @returns {Promise.<string[]>} full paths
 */
export async function findImportCandidates(dataDirectory, customerKey, format) {
    let entries;
    try {
        entries = await fs.readdir(dataDirectory, { withFileTypes: true });
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
        const fileExtension = path.extname(name).toLowerCase().slice(1);
        if (!extensions.includes(fileExtension)) {
            continue;
        }
        try {
            const { customerKey: parsedKey } = parseExportBasename(name);
            if (parsedKey === customerKey) {
                matches.push(path.join(dataDirectory, name));
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
        if (!(t >= bestTime)) {
            continue;
        }

        bestTime = t;
        best = p;
    }
    return best;
}
