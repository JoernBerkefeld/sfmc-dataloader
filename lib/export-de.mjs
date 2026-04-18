import { createWriteStream } from 'node:fs';
import fs from 'node:fs/promises';
import path from 'node:path';
import { finished } from 'node:stream/promises';
import { stringify } from 'csv-stringify';
import { stringify as stringifySync } from 'csv-stringify/sync';
import { rowsetGetPath } from './import-routes.mjs';
import { buildExportBasename, filesystemSafeTimestamp } from './filename.mjs';
import { dataDirectoryForBu } from './paths.mjs';

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
    const rows = [];
    for (const item of items) {
        rows.push({ ...item.keys, ...item.values });
    }
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
 * @param {{ rest: object, soap: { retrieve: Function } }} sdk
 * @param {object} params
 * @param {string} params.projectRoot
 * @param {string} params.credentialName
 * @param {string} params.buName
 * @param {string} params.deKey
 * @param {'csv'|'tsv'|'json'} params.format
 * @param {boolean} [params.jsonPretty]
 * @param {boolean} [params.useGit]
 * @param {number} [params.maxRowsPerFile] - split output into part files with at most this many data rows each
 * @returns {Promise.<{paths: string[], rowCount: number}>}
 */
export async function exportDataExtensionToFile(sdk, params) {
    const {
        projectRoot,
        credentialName,
        buName,
        deKey,
        format,
        jsonPretty = false,
        useGit = false,
        maxRowsPerFile,
    } = params;
    const dir = dataDirectoryForBu(projectRoot, credentialName, buName);
    await fs.mkdir(dir, { recursive: true });
    const ts = filesystemSafeTimestamp();
    const basePath = rowsetGetPath(deKey);
    const cap =
        typeof maxRowsPerFile === 'number' && maxRowsPerFile > 0 ? maxRowsPerFile : undefined;

    /** @type {string[]} */
    const paths = [];
    let totalRows = 0;

    if (format === 'json') {
        let partIndex = 0;
        /** @type {import('node:fs').WriteStream|null} */
        let writeStream = null;
        let rowsInPart = 0;
        let firstInArray = true;

        const closeJsonFile = async () => {
            if (writeStream) {
                writeStream.write('\n]\n');
                writeStream.end();
                await finished(writeStream);
                writeStream = null;
            }
            firstInArray = true;
            rowsInPart = 0;
        };

        const openJsonPart = async () => {
            await closeJsonFile();
            partIndex++;
            const basename = cap
                ? buildExportBasename(deKey, ts, format, useGit, partIndex)
                : buildExportBasename(deKey, ts, format, useGit);
            const outPath = path.join(dir, basename);
            paths.push(outPath);
            writeStream = createWriteStream(outPath, { encoding: 'utf8' });
            writeStream.write('[\n');
            firstInArray = true;
        };

        try {
            for await (const step of sdk.rest.getBulkPages(basePath, 2500)) {
                if (step.totalPages === undefined) {
                    process.stdout.write(
                        ` - Requesting next batch (currently ${totalRows} records)\n`,
                    );
                } else {
                    process.stdout.write(
                        ` - Requesting batch ${step.page} of ${step.totalPages} (${totalRows} records so far)\n`,
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
                    if (firstInArray) {
                        firstInArray = false;
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
                console.error(
                    `Warning: could not retrieve field names for empty DE "${deKey}": ${ex.message}`,
                );
            }
            const basename = buildExportBasename(deKey, ts, format, useGit);
            const outPath = path.join(dir, basename);
            const body = serializeRows([], format, jsonPretty, columns);
            await fs.writeFile(outPath, body, 'utf8');
            paths.push(outPath);
        }

        return { paths, rowCount: totalRows };
    }

    /** @type {string[]|null} */
    let columnNames = null;
    let partIndex = 0;
    /** @type {import('stream').Transform|null} */
    let stringifier = null;
    /** @type {import('node:fs').WriteStream|null} */
    let writeStream = null;
    let rowsInPart = 0;
    let isFirstCsvFile = true;

    const closeCsvPart = async () => {
        if (stringifier && writeStream) {
            stringifier.end();
            await finished(writeStream);
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
        const outPath = path.join(dir, basename);
        paths.push(outPath);
        writeStream = createWriteStream(outPath, { encoding: 'utf8' });
        const includeHeader = isFirstCsvFile;
        isFirstCsvFile = false;
        stringifier = stringify({
            header: includeHeader,
            bom: includeHeader,
            quoted: format === 'csv',
            delimiter: format === 'tsv' ? '\t' : ',',
            ...(columnNames && columnNames.length > 0 ? { columns: columnNames } : {}),
        });
        stringifier.pipe(writeStream);
    };

    try {
        for await (const step of sdk.rest.getBulkPages(basePath, 2500)) {
            if (step.totalPages === undefined) {
                process.stdout.write(` - Requesting next batch (currently ${totalRows} records)\n`);
            } else {
                process.stdout.write(
                    ` - Requesting batch ${step.page} of ${step.totalPages} (${totalRows} records so far)\n`,
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
                stringifier.write(row);
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
        await closeCsvPart();
    }

    if (paths.length === 0) {
        let columns = [];
        try {
            columns = await fetchDataExtensionFieldNames(sdk.soap, deKey);
        } catch (ex) {
            console.error(
                `Warning: could not retrieve field names for empty DE "${deKey}": ${ex.message}`,
            );
        }
        columnNames = columns.length > 0 ? columns : null;
        await openCsvPart();
        stringifier.end();
        await finished(writeStream);
        stringifier = null;
        writeStream = null;
    }

    return { paths, rowCount: totalRows };
}
