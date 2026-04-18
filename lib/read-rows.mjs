import { createReadStream, promises as fsPromises } from 'node:fs';
import csv from 'csv-parser';

/**
 * @param {string} filePath
 * @param {'csv'|'tsv'|'json'} format
 * @param {object} [options]
 * @param {string[]} [options.columnHeaders] - when set (CSV/TSV), the file has no header row; map columns by position
 * @returns {Promise.<object[]>}
 */
export async function readRowsFromFile(filePath, format, options = {}) {
    const { columnHeaders } = options;
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
        const parserOptions = {
            separator: delimiter,
            bom: true,
            mapHeaders: ({ header }) => {
                let h = header;
                if (h.startsWith('\uFEFF')) {
                    h = h.slice(1);
                }
                if (h.startsWith('"') && h.endsWith('"') && h.length >= 2) {
                    h = h.slice(1, -1);
                }
                return h;
            },
            mapValues: ({ value }) => {
                if (value.startsWith('"') && value.endsWith('"') && value.length >= 2) {
                    return value.slice(1, -1);
                }
                return value;
            },
        };
        if (columnHeaders && columnHeaders.length > 0) {
            parserOptions.headers = columnHeaders;
        }
        createReadStream(filePath)
            .pipe(csv(parserOptions))
            .on('data', (row) => rows.push(row))
            .on('end', () => resolve(rows))
            .on('error', reject);
    });
}

/**
 * Reads one or more export parts in order. CSV/TSV multi-part exports from mcdata include a header
 * row only in the first file; continuation files are data-only and require inferred column names.
 *
 * @param {string[]} filePaths
 * @param {'csv'|'tsv'|'json'} format
 * @returns {Promise.<object[]>}
 */
export async function readRowsFromImportPaths(filePaths, format) {
    if (filePaths.length === 0) {
        throw new Error('readRowsFromImportPaths requires at least one file path');
    }
    if (format === 'json') {
        const all = [];
        for (const fp of filePaths) {
            all.push(...(await readRowsFromFile(fp, 'json')));
        }
        return all;
    }
    const firstRows = await readRowsFromFile(filePaths[0], format);
    if (filePaths.length === 1) {
        return firstRows;
    }
    const headers = firstRows.length > 0 ? Object.keys(firstRows[0]) : [];
    if (headers.length === 0) {
        throw new Error(
            `Cannot import multi-part ${format}: the first file has no data rows to infer columns from.`,
        );
    }
    const rest = [];
    for (let i = 1; i < filePaths.length; i++) {
        rest.push(
            ...(await readRowsFromFile(filePaths[i], format, {
                columnHeaders: headers,
            })),
        );
    }
    return [...firstRows, ...rest];
}
