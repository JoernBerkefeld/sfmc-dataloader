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
    const data = await sdk.rest.getBulk(basePath, 2500);
    const items = data.items ?? [];
    const rows = [];
    for (const item of items) {
        rows.push({ ...item.keys, ...item.values });
    }
    return rows;
}

/**
 * @param {object[]} rows
 * @param {'csv'|'tsv'|'json'} format
 * @param {boolean} jsonPretty
 * @returns {string}
 */
export function serializeRows(rows, format, jsonPretty) {
    if (format === 'json') {
        const space = jsonPretty ? 2 : undefined;
        return JSON.stringify(rows, null, space) + '\n';
    }
    const delimiter = format === 'tsv' ? '\t' : ',';
    return stringify(rows, {
        header: true,
        quoted: true,
        bom: true,
        delimiter,
    });
}

/**
 * @param {{rest: {getBulk: (path: string, pageSize?: number) => Promise.<any>}}} sdk
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
    const dir = dataDirectoryForBu(projectRoot, credentialName, buName);
    await fs.mkdir(dir, { recursive: true });
    const ts = filesystemSafeTimestamp();
    const basename = buildExportBasename(deKey, ts, format, useGit);
    const outPath = path.join(dir, basename);
    const body = serializeRows(rows, format, jsonPretty);
    await fs.writeFile(outPath, body, 'utf8');
    return { path: outPath, rowCount: rows.length };
}
