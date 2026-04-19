import { createReadStream, promises as fsPromises } from 'node:fs';
import csv from 'csv-parser';

/**
 * @param {'csv'|'tsv'} format
 * @param {string[]} [columnHeaders]
 * @returns {object}
 */
function buildCsvParserOptions(format, columnHeaders) {
    const delimiter = format === 'tsv' ? '\t' : ',';
    /** @type {object} */
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
    return parserOptions;
}

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
    const parserOptions = buildCsvParserOptions(format, columnHeaders);
    return new Promise((resolve, reject) => {
        const rows = [];
        createReadStream(filePath)
            .pipe(csv(parserOptions))
            .on('data', (row) => rows.push(row))
            .on('end', () => resolve(rows))
            .on('error', reject);
    });
}

/**
 * Streams CSV/TSV rows from disk (one row at a time). JSON reads the full file then yields each row.
 *
 * @param {string} filePath
 * @param {'csv'|'tsv'|'json'} format
 * @param {object} [options]
 * @param {string[]} [options.columnHeaders]
 * @yields {object} one row object per iteration
 * @returns {AsyncGenerator<object, void, void>}
 */
export async function* streamRowsFromFile(filePath, format, options = {}) {
    const { columnHeaders } = options;
    if (format === 'json') {
        const rows = await readRowsFromFile(filePath, 'json');
        for (const row of rows) {
            yield row;
        }
        return;
    }
    const parserOptions = buildCsvParserOptions(format, columnHeaders);
    const stream = createReadStream(filePath).pipe(csv(parserOptions));
    for await (const row of stream) {
        yield row;
    }
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

/**
 * Counts data rows without retaining row objects (two passes vs import: count then stream).
 *
 * @param {string[]} filePaths
 * @param {'csv'|'tsv'|'json'} format
 * @returns {Promise.<number>}
 */
export async function countDataRowsFromImportPaths(filePaths, format) {
    let n = 0;
    for await (const _row of streamRowsFromImportPaths(filePaths, format)) {
        n += 1;
    }
    return n;
}

export async function* streamRowsFromImportPaths(filePaths, format) {
    if (filePaths.length === 0) {
        throw new Error('streamRowsFromImportPaths requires at least one file path');
    }
    if (format === 'json') {
        for (const fp of filePaths) {
            yield* streamRowsFromFile(fp, 'json');
        }
        return;
    }
    /** @type {string[]|null} */
    let headers = null;
    for (let i = 0; i < filePaths.length; i++) {
        if (i === 0) {
            let sawRow = false;
            for await (const row of streamRowsFromFile(filePaths[0], format)) {
                if (!sawRow) {
                    headers = Object.keys(row);
                    sawRow = true;
                }
                yield row;
            }
            if (filePaths.length > 1 && (!headers || headers.length === 0)) {
                throw new Error(
                    `Cannot import multi-part ${format}: the first file has no data rows to infer columns from.`,
                );
            }
        } else {
            yield* streamRowsFromFile(filePaths[i], format, {
                columnHeaders: /** @type {string[]} */ (headers),
            });
        }
    }
}
