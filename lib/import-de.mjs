import { chunkItemsForPayload } from './batch.mjs';
import { formatFromExtension } from './file-resolve.mjs';
import { resolveImportRoute } from './import-routes.mjs';
import { withRetry429 } from './retry.mjs';
import { readRowsFromFile } from './read-rows.mjs';

/**
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {object} params
 * @param {string} params.deKey
 * @param {object[]} params.rows
 * @param {'upsert'|'insert'} params.mode
 * @returns {Promise.<number>} number of rows imported
 */
export async function importRowsForDe(sdk, params) {
    const { deKey, rows, mode } = params;
    const route = resolveImportRoute(mode);
    const chunks = chunkItemsForPayload(rows);
    for (const chunk of chunks) {
        const p = route.path(deKey);
        const body = { items: chunk };
        await withRetry429(() =>
            route.method === 'PUT' ? sdk.rest.put(p, body) : sdk.rest.post(p, body),
        );
    }
    return rows.length;
}

/**
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {object} params
 * @param {string} params.filePath
 * @param {string} params.deKey - target DE customer key for API
 * @param {'csv'|'tsv'|'json'} [params.format] - optional; auto-detected from file extension if omitted
 * @param {'upsert'|'insert'} params.mode
 * @returns {Promise.<number>} number of rows imported
 */
export async function importFromFile(sdk, params) {
    const format = params.format || formatFromExtension(params.filePath);
    if (!format) {
        throw new Error(
            `Cannot determine format for file: ${params.filePath}. Use .csv, .tsv, or .json extension.`,
        );
    }
    const rows = await readRowsFromFile(params.filePath, format);
    return importRowsForDe(sdk, {
        deKey: params.deKey,
        rows,
        mode: params.mode,
    });
}
