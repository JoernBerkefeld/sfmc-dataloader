import fs from 'node:fs/promises';
import path from 'node:path';
import { filterIllegalFilenames, MCDATA_SENTINEL } from './filename.mjs';

/**
 * Find export files under data dir matching encoded DE key prefix and extension.
 *
 * @param {string} dataDir
 * @param {string} customerKey
 * @param {'csv'|'tsv'|'json'} format
 * @returns {Promise<string[]>} full paths
 */
export async function findImportCandidates(dataDir, customerKey, format) {
    const prefix = filterIllegalFilenames(customerKey) + MCDATA_SENTINEL;
    let entries;
    try {
        entries = await fs.readdir(dataDir, { withFileTypes: true });
    } catch {
        return [];
    }
    const ext = format;
    const matches = [];
    for (const ent of entries) {
        if (!ent.isFile()) {
            continue;
        }
        const name = ent.name;
        if (!name.endsWith(`.${ext}`)) {
            continue;
        }
        if (name.startsWith(prefix)) {
            matches.push(path.join(dataDir, name));
        }
    }
    return matches;
}

/**
 * @param {string[]} paths
 * @returns {Promise<string>} path with newest mtime
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
