import { createReadStream, promises as fsPromises } from 'node:fs';
import csv from 'csv-parser';

/**
 * @param {string} filePath
 * @param {'csv'|'tsv'|'json'} format
 * @returns {Promise<object[]>}
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
                })
            )
            .on('data', (row) => rows.push(row))
            .on('end', () => resolve(rows))
            .on('error', reject);
    });
}
