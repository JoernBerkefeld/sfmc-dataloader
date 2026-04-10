import { createReadStream, promises as fsPromises } from 'node:fs';
import csv from 'csv-parser';

/**
 * @param {string} filePath
 * @param {'csv'|'tsv'|'json'} format
 * @returns {Promise.<object[]>}
 */
export async function readRowsFromFile(filePath, format) {
    if (format === 'json') {
        const raw = await fsPromises.readFile(filePath, 'utf8');
        const parsed = JSON.parse(raw);
        if (Array.isArray(parsed)) {
            return parsed;
        }
        if (parsed && typeof parsed === 'object' && Array.isArray(parsed.items)) {
            return parsed.items;
        }
        throw new Error('JSON import must be an array of row objects or { "items": [...] }');
    }
    const delimiter = format === 'tsv' ? '\t' : ',';
    return new Promise((resolve, reject) => {
        const rows = [];
        createReadStream(filePath)
            .pipe(
                csv({
                    separator: delimiter,
                    bom: true,
                    mapHeaders: ({ header }) => {
                        let h = header;
                        // Strip BOM if present (backup in case bom:true misses it)
                        if (h.codePointAt(0) === 0xFEFF) {
                            h = h.slice(1);
                        }
                        // Strip surrounding quotes if present (non-standard quoted TSV)
                        if (h.startsWith('"') && h.endsWith('"') && h.length >= 2) {
                            h = h.slice(1, -1);
                        }
                        return h;
                    },
                    mapValues: ({ value }) => {
                        // Strip surrounding quotes from values if present
                        if (value.startsWith('"') && value.endsWith('"') && value.length >= 2) {
                            return value.slice(1, -1);
                        }
                        return value;
                    },
                }),
            )
            .on('data', (row) => rows.push(row))
            .on('end', () => resolve(rows))
            .on('error', reject);
    });
}
