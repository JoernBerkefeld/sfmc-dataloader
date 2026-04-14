import fs from 'node:fs/promises';
import path from 'node:path';
import { stringify } from 'csv-stringify/sync';
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
    return stringify(rows, options);
}

/**
 * @param {{ rest: { getBulk: (path: string, pageSize?: number) => Promise.<any> }, soap: { retrieve: Function } }} sdk
 * @param {object} params
 * @param {string} params.projectRoot
 * @param {string} params.credentialName
 * @param {string} params.buName
 * @param {string} params.deKey
 * @param {'csv'|'tsv'|'json'} params.format
 * @param {boolean} [params.jsonPretty]
 * @param {boolean} [params.useGit]
 * @returns {Promise.<{path: string, rowCount: number}>}
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
    } = params;
    const rows = await fetchAllRowObjects(sdk, deKey);
    let columns = [];
    if (rows.length === 0 && format !== 'json') {
        try {
            columns = await fetchDataExtensionFieldNames(sdk.soap, deKey);
        } catch (ex) {
            console.error(
                `Warning: could not retrieve field names for empty DE "${deKey}": ${ex.message}`,
            );
        }
    }
    const dir = dataDirectoryForBu(projectRoot, credentialName, buName);
    await fs.mkdir(dir, { recursive: true });
    const ts = filesystemSafeTimestamp();
    const basename = buildExportBasename(deKey, ts, format, useGit);
    const outPath = path.join(dir, basename);
    const body = serializeRows(rows, format, jsonPretty, columns);
    await fs.writeFile(outPath, body, 'utf8');
    return { path: outPath, rowCount: rows.length };
}
