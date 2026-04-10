import fs from 'node:fs/promises';
import path from 'node:path';
import { parseExportBasename } from './filename.mjs';

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
