import { createReadStream, promises as fsPromises } from 'node:fs';
import { pipeline } from 'node:stream/promises';
import csv, { validateFile } from '@joern.berkefeld/csv-parser';

/**
 * @param {'csv'|'tsv'} format
 * @param {string[]} [columnHeaders]
 * @param {object} [options]
 * @param {boolean} [options.skipEmptyLines]
 * @returns {object}
 */
function buildCsvParserOptions(format, columnHeaders, options = {}) {
    return {
        separator: format === 'tsv' ? '\t' : ',',
        strict: true,
        strictQuotes: true,
        bom: true,
        skipEmptyLines: options.skipEmptyLines === true,
        ...(columnHeaders?.length && { headers: columnHeaders }),
    };
}

/**
 * @param {string} filePath
 * @param {'csv'|'tsv'|'json'} format
 * @param {object} [options]
 * @param {boolean} [options.skipEmptyLines] - Ignored for JSON.
 * @param {string[]} [options.columnHeaders] - Positional headers for data-only continuation files.
 * @returns {Promise.<object[]>}
 */
export async function readRowsFromFile(filePath, format, options = {}) {
    if (format !== 'json') {
        return Array.fromAsync(streamRowsFromFile(filePath, format, options));
    }
    const parsed = JSON.parse(await fsPromises.readFile(filePath, 'utf8'));
    if (Array.isArray(parsed)) {
        return parsed;
    }
    if (parsed && typeof parsed === 'object' && Array.isArray(parsed.items)) {
        return parsed.items;
    }
    throw new Error('JSON import must be an array of row objects or { "items": [...] }');
}

/**
 * Streams delimited rows; owned streams close on errors and early iterator return.
 *
 * @param {string} filePath
 * @param {'csv'|'tsv'|'json'} format
 * @param {object} [options]
 * @param {boolean} [options.skipEmptyLines] - Ignored for JSON.
 * @param {string[]} [options.columnHeaders]
 * @yields {object} One row at a time.
 * @returns {AsyncGenerator<object, void, void>}
 */
export async function* streamRowsFromFile(filePath, format, options = {}) {
    if (format === 'json') {
        yield* await readRowsFromFile(filePath, format);
        return;
    }
    const parser = csv(buildCsvParserOptions(format, options.columnHeaders, options));
    const source = createReadStream(filePath);
    // Observe pipeline failure immediately, including source errors before iteration begins.
    // eslint-disable-next-line unicorn/prefer-await -- Attach a rejection handler before consuming the parser.
    const completion = pipeline(source, parser).catch((ex) => ex);
    try {
        yield* parser;
        const failure = await completion;
        if (failure) {
            throw failure;
        }
    } catch (ex) {
        throw new Error(`Unable to read ${filePath}: ${ex.message}`, { cause: ex });
    } finally {
        source.destroy();
        parser.destroy();
        await completion;
    }
}

/**
 * Validate an ordered logical import set completely, without retaining delimited rows.
 * Inputs are assumed unchanged between preflight and consumption.
 *
 * @param {string[]} filePaths
 * @param {'csv'|'tsv'|'json'} format
 * @param {object} [options]
 * @param {boolean} [options.skipEmptyLines] - Skip unquoted blank/whitespace-only records.
 * @returns {Promise<{paths: readonly string[], format: string, headers: readonly string[], recordCount: number, skipEmptyLines: boolean}>}
 */
export async function preflightImportPaths(filePaths, format, options = {}) {
    if (filePaths.length === 0) {
        throw new Error('preflightImportPaths requires at least one file path');
    }
    const paths = [...filePaths];
    const isSkipEmptyLines = format !== 'json' && options.skipEmptyLines === true;
    let headers = [];
    let recordCount = 0;
    for (const [index, filePath] of paths.entries()) {
        if (format === 'json') {
            recordCount += (await readRowsFromFile(filePath, format)).length;
        } else {
            const result = await validateFile(
                filePath,
                buildCsvParserOptions(format, index === 0 ? undefined : headers, {
                    skipEmptyLines: isSkipEmptyLines,
                }),
            );
            if (index === 0) {
                headers = result.headers ?? [];
                if (headers.length === 0) {
                    throw new Error(
                        `Import file has no data rows and no column headers: ${filePath}`,
                    );
                }
            }
            recordCount += result.recordCount;
        }
    }
    if (recordCount === 0) {
        throw new Error('Import set has no data rows');
    }
    return Object.freeze({
        paths: Object.freeze(paths),
        format,
        headers: Object.freeze(headers),
        recordCount,
        skipEmptyLines: isSkipEmptyLines,
    });
}

/**
 * @param {string[]} filePaths
 * @param {'csv'|'tsv'|'json'} format
 * @param {object} [metadata] - Authoritative preflight result for these unchanged inputs/options.
 * @param {object} [options] - Used only when metadata is omitted.
 * @param {boolean} [options.skipEmptyLines]
 * @returns {Promise.<object[]>}
 */
export async function readRowsFromImportPaths(filePaths, format, metadata, options = {}) {
    return Array.fromAsync(streamRowsFromImportPaths(filePaths, format, metadata, options));
}

/**
 * Count during full validation, or reuse already validated metadata without another scan.
 *
 * @param {string[]} filePaths
 * @param {'csv'|'tsv'|'json'} format
 * @param {object} [metadata] - Authoritative preflight result for these unchanged inputs/options.
 * @param {object} [options] - Used only when metadata is omitted.
 * @param {boolean} [options.skipEmptyLines]
 * @returns {Promise.<number>}
 */
export async function countDataRowsFromImportPaths(filePaths, format, metadata, options = {}) {
    return (metadata ?? (await preflightImportPaths(filePaths, format, options))).recordCount;
}

/**
 * @param {string[]} filePaths
 * @param {'csv'|'tsv'|'json'} format
 * @param {object} [metadata] - Authoritative preflight result for these unchanged inputs/options.
 * @param {object} [options] - Used only when metadata is omitted.
 * @param {boolean} [options.skipEmptyLines]
 * @yields {object} One row at a time.
 * @returns {AsyncGenerator<object, void, void>}
 */
export async function* streamRowsFromImportPaths(filePaths, format, metadata, options = {}) {
    const prepared = metadata ?? (await preflightImportPaths(filePaths, format, options));
    for (const [index, filePath] of prepared.paths.entries()) {
        yield* streamRowsFromFile(filePath, prepared.format, {
            columnHeaders: index === 0 ? undefined : prepared.headers,
            skipEmptyLines: prepared.skipEmptyLines,
        });
    }
}
