import { createWriteStream } from 'node:fs';
import fs from 'node:fs/promises';
import path from 'node:path';
import process from 'node:process';
import { finished, pipeline } from 'node:stream/promises';
import { once } from 'node:events';
import { stringify } from 'csv-stringify';
import { stringify as stringifySync } from 'csv-stringify/sync';
import { rowsetGetPath } from './import-routes.mjs';
import { buildExportBasename, filesystemSafeTimestamp } from './filename.mjs';
import { dataDirectoryForBu } from './paths.mjs';
import { log } from './log.mjs';

/**
 * @param {{rest: {getBulk: (path: string, pageSize?: number) => Promise.<any>}}} sdk
 * @param {string} deKey
 * @returns {Promise.<object[]>}
 */
export async function fetchAllRowObjects(sdk, deKey) {
    const basePath = rowsetGetPath(deKey);
    let data;
    try {
        data = await sdk.rest.getBulk(basePath, 2500);
    } catch (ex) {
        // this api endpoint won't return "items" if the dataExtension is empty
        if (ex.message !== 'Could not find an array to iterate over') {
            throw ex;
        }
        data = { items: [] };
    }
    const items = data.items ?? [];
    const rows = Array.from(items, (item) => ({ ...item.keys, ...item.values }));
    return rows;
}

/**
 * Field names for a Data Extension, in ordinal order (SOAP).
 *
 * @param {{ retrieve: (type: string, props: string[], opts: object) => Promise.<any> }} soap
 * @param {string} deKey - DE customer key
 * @returns {Promise.<string[]>}
 */
export async function fetchDataExtensionFieldNames(soap, deKey) {
    const result = await soap.retrieve('DataExtensionField', ['Name', 'Ordinal'], {
        filter: {
            leftOperand: 'DataExtension.CustomerKey',
            operator: 'equals',
            rightOperand: deKey,
        },
    });
    const fields = result?.Results;
    if (!Array.isArray(fields) || fields.length === 0) {
        return [];
    }
    return fields
        .toSorted((a, b) => Number(a.Ordinal ?? 0) - Number(b.Ordinal ?? 0))
        .map((f) => f.Name);
}

/**
 * @param {object[]} rows
 * @param {'csv'|'tsv'|'json'} format
 * @param {boolean} jsonPretty
 * @param {string[]} [columns] - when `rows` is empty and format is csv/tsv, emit this header row
 * @returns {string}
 */
export function serializeRows(rows, format, jsonPretty, columns = []) {
    if (format === 'json') {
        const space = jsonPretty ? 2 : undefined;
        return JSON.stringify(rows, null, space) + '\n';
    }
    const delimiter = format === 'tsv' ? '\t' : ',';
    const options = {
        header: true,
        quoted: format === 'csv',
        bom: true,
        delimiter,
    };
    if (rows.length === 0 && columns.length > 0) {
        options.columns = columns;
    }
    return stringifySync(rows, options);
}

/**
 * @param {string} value
 * @returns {string}
 */
function normalize(value) {
    const absolute = path.resolve(value);
    return process.platform === 'win32' ? absolute.toLowerCase() : absolute;
}

/**
 * Protect frozen import sources from backup writers, including aliases and hardlinks.
 * Internal helper shared by import orchestration; not exported from the package entry point.
 *
 * @param {string[]} inputPaths
 * @returns {Promise.<(outPath: string) => Promise.<void>>}
 */
export async function createBackupPathGuard(inputPaths) {
    const inputs = await Promise.all(
        inputPaths.map(async (inputPath) => ({
            path: normalize(inputPath),
            realPath: normalize(await fs.realpath(inputPath)),
            identity: await fs.stat(inputPath, { bigint: true }),
        })),
    );
    return async (outPath) => {
        let realPath;
        let identity;
        try {
            realPath = normalize(await fs.realpath(outPath));
            identity = await fs.stat(outPath, { bigint: true });
        } catch (ex) {
            if (ex.code !== 'ENOENT') {
                throw ex;
            }
        }
        if (
            inputs.some(
                (input) =>
                    input.path === normalize(outPath) ||
                    input.realPath === realPath ||
                    (identity &&
                        input.identity.dev === identity.dev &&
                        input.identity.ino === identity.ino),
            )
        ) {
            throw new Error(
                `Backup destination collides with a selected import source: "${outPath}".`,
            );
        }
    };
}

/**
 * @param {{ rest: object, soap: { retrieve: Function } }} sdk
 * @param {object} parameters
 * @param {string} parameters.projectRoot
 * @param {string} parameters.credentialName
 * @param {string} parameters.buName
 * @param {string} parameters.deKey
 * @param {'csv'|'tsv'|'json'} parameters.format
 * @param {boolean} [parameters.jsonPretty]
 * @param {boolean} [parameters.useGit]
 * @param {number} [parameters.maxRowsPerFile] - split output into part files with at most this many data rows each
 * @param {(outPath: string) => Promise.<void>} [parameters.beforeOpen] - internal backup safety check before each writer opens
 * @returns {Promise.<{paths: string[], rowCount: number}>}
 */
export async function exportDataExtensionToFile(sdk, parameters) {
    const {
        projectRoot,
        credentialName,
        buName,
        deKey,
        format,
        jsonPretty = false,
        useGit = false,
        maxRowsPerFile,
        beforeOpen,
    } = parameters;
    const directory = dataDirectoryForBu(projectRoot, credentialName, buName);
    await fs.mkdir(directory, { recursive: true });
    const ts = filesystemSafeTimestamp();
    const basePath = rowsetGetPath(deKey);
    const cap =
        typeof maxRowsPerFile === 'number' && maxRowsPerFile > 0 ? maxRowsPerFile : undefined;

    /**
     * @type {string[]}
     */
    const paths = [];
    let totalRows = 0;

    if (format === 'json') {
        let partIndex = 0;
        /**
         * @type {import('node:fs').WriteStream|null}
         */
        let writeStream = null;
        let rowsInPart = 0;
        let isFirstInArray = true;

        const closeJsonFile = async () => {
            if (writeStream) {
                writeStream.write('\n]\n');
                writeStream.end();
                await finished(writeStream);
                writeStream = null;
            }
            isFirstInArray = true;
            rowsInPart = 0;
        };

        const openJsonPart = async () => {
            await closeJsonFile();
            partIndex++;
            const basename = cap
                ? buildExportBasename(deKey, ts, format, useGit, partIndex)
                : buildExportBasename(deKey, ts, format, useGit);
            const outPath = path.join(directory, basename);
            await beforeOpen?.(outPath);
            paths.push(outPath);
            writeStream = createWriteStream(outPath, { encoding: 'utf8' });
            writeStream.write('[\n');
            isFirstInArray = true;
        };

        try {
            for await (const step of sdk.rest.getBulkPages(basePath, 2500)) {
                if (step.totalPages === undefined) {
                    log.info(`Downloading next batch (currently ${totalRows} records)`);
                } else {
                    log.info(
                        `Downloading batch ${step.page} of ${step.totalPages} (${totalRows} records so far)`,
                    );
                }
                for (const item of step.pageItems) {
                    const row = { ...item.keys, ...item.values };
                    if (writeStream === null) {
                        await openJsonPart();
                    }
                    if (cap && rowsInPart >= cap) {
                        await openJsonPart();
                    }
                    if (isFirstInArray) {
                        isFirstInArray = false;
                    } else {
                        writeStream.write(',\n');
                    }
                    const chunk = jsonPretty ? JSON.stringify(row, null, 2) : JSON.stringify(row);
                    writeStream.write(chunk);
                    rowsInPart++;
                    totalRows++;
                }
            }
        } catch (ex) {
            if (ex.message !== 'Could not find an array to iterate over') {
                throw ex;
            }
        }

        if (writeStream) {
            await closeJsonFile();
        }

        if (paths.length === 0) {
            let columns = [];
            try {
                columns = await fetchDataExtensionFieldNames(sdk.soap, deKey);
            } catch (ex) {
                log.warn(
                    `Warning: could not retrieve field names for empty DE "${deKey}": ${ex.message}`,
                );
            }
            const basename = buildExportBasename(deKey, ts, format, useGit);
            const outPath = path.join(directory, basename);
            const body = serializeRows([], format, jsonPretty, columns);
            await beforeOpen?.(outPath);
            await fs.writeFile(outPath, body, 'utf8');
            paths.push(outPath);
        }

        return { paths, rowCount: totalRows };
    }

    /**
     * @type {string[]|null}
     */
    let columnNames = null;
    let partIndex = 0;
    /**
     * @type {import('stream').Transform|null}
     */
    let stringifier = null;
    /**
     * @type {import('node:fs').WriteStream|null}
     */
    let writeStream = null;
    let rowsInPart = 0;
    let isFirstCsvFile = true;
    let partCompletion;
    let streamFailure;
    const failure = new AbortController();

    // Observe SDK requests even when output fails before a pending request settles.
    const awaitOrFailure = async (operation) => {
        let onFailure;
        const aborted = new Promise((_, reject) => {
            onFailure = () => reject(streamFailure);
            failure.signal.addEventListener('abort', onFailure, { once: true });
            if (failure.signal.aborted) {
                onFailure();
            }
        });
        try {
            return await Promise.race([operation, aborted]);
        } finally {
            failure.signal.removeEventListener('abort', onFailure);
        }
    };

    const closeCsvPart = async () => {
        if (stringifier && writeStream) {
            stringifier.end();
            await partCompletion;
            if (streamFailure) {
                throw streamFailure;
            }
        }
        stringifier = null;
        writeStream = null;
        rowsInPart = 0;
    };

    const openCsvPart = async () => {
        await closeCsvPart();
        partIndex++;
        const basename = cap
            ? buildExportBasename(deKey, ts, format, useGit, partIndex)
            : buildExportBasename(deKey, ts, format, useGit);
        const outPath = path.join(directory, basename);
        await beforeOpen?.(outPath);
        paths.push(outPath);
        writeStream = createWriteStream(outPath, { encoding: 'utf8' });
        const includeHeader = isFirstCsvFile;
        isFirstCsvFile = false;
        stringifier = stringify({
            header: includeHeader,
            bom: includeHeader,
            quoted: format === 'csv',
            delimiter: format === 'tsv' ? '\t' : ',',
            ...(columnNames && columnNames.length > 0 && { columns: columnNames }),
        });
        // Install error/close observation before the first write, including final flush.
        const completion = pipeline(stringifier, writeStream);
        partCompletion = (async () => {
            try {
                await completion;
            } catch (ex) {
                streamFailure ??= ex;
                failure.abort(streamFailure);
            }
        })();
    };

    const pages = sdk.rest.getBulkPages(basePath, 2500)[Symbol.asyncIterator]();
    let isPagesComplete = false;
    try {
        while (true) {
            failure.signal.throwIfAborted();
            let next;
            try {
                next = await awaitOrFailure(pages.next());
            } catch (ex) {
                if (streamFailure || ex.message !== 'Could not find an array to iterate over') {
                    throw ex;
                }
                next = { done: true };
            }
            if (next.done) {
                isPagesComplete = true;
                break;
            }
            const step = next.value;
            if (step.totalPages === undefined) {
                log.info(`Downloading next batch (currently ${totalRows} records)`);
            } else {
                log.info(
                    `Downloading batch ${step.page} of ${step.totalPages} (${totalRows} records so far)`,
                );
            }
            for (const item of step.pageItems) {
                const row = { ...item.keys, ...item.values };
                if (columnNames === null && Object.keys(row).length > 0) {
                    columnNames = Object.keys(row);
                }
                if (writeStream === null) {
                    await openCsvPart();
                }
                if (cap && rowsInPart >= cap) {
                    await openCsvPart();
                }
                failure.signal.throwIfAborted();
                if (!stringifier.write(row)) {
                    await awaitOrFailure(once(stringifier, 'drain', { signal: failure.signal }));
                }
                rowsInPart++;
                totalRows++;
            }
        }

        if (writeStream) {
            await closeCsvPart();
        }

        if (paths.length === 0) {
            let columns = [];
            try {
                columns = await fetchDataExtensionFieldNames(sdk.soap, deKey);
            } catch (ex) {
                log.warn(
                    `Warning: could not retrieve field names for empty DE "${deKey}": ${ex.message}`,
                );
            }
            columnNames = columns.length > 0 ? columns : null;
            await openCsvPart();
            await closeCsvPart();
        }
    } catch (ex) {
        const cause = streamFailure ?? ex;
        stringifier?.destroy();
        writeStream?.destroy();
        await partCompletion;
        throw cause;
    } finally {
        if (!isPagesComplete && pages.return) {
            // An SDK generator may be blocked in next(); never await its queued return().
            void (async () => {
                try {
                    await pages.return();
                } catch {
                    // The original export failure remains authoritative.
                }
            })();
        }
    }

    return { paths, rowCount: totalRows };
}
