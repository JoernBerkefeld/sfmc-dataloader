import { chunkItemsForPayload } from './batch.mjs';
import { resolveImportRoute } from './import-routes.mjs';
import { withRetry429 } from './retry.mjs';
import { readRowsFromFile } from './read-rows.mjs';

/**
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {object} params
 * @param {string} params.deKey
 * @param {object[]} params.rows
 * @param {'async'|'sync'} params.api
 * @param {'upsert'|'insert'|'update'} params.mode
 * @returns {Promise<void>}
 */
export async function importRowsForDe(sdk, params) {
    const { deKey, rows, api, mode } = params;
    const route = resolveImportRoute(api, mode);
    const chunks = chunkItemsForPayload(rows);
    for (const chunk of chunks) {
        const path = route.path(deKey);
        const body = { items: chunk };
        await withRetry429(() =>
            route.method === 'PUT'
                ? sdk.rest.put(path, body)
                : sdk.rest.post(path, body)
        );
    }
}

/**
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {object} params
 * @param {string} params.filePath
 * @param {string} params.deKey - target DE customer key for API
 * @param {'csv'|'tsv'|'json'} params.format
 * @param {'async'|'sync'} params.api
 * @param {'upsert'|'insert'|'update'} params.mode
 * @returns {Promise<void>}
 */
export async function importFromFile(sdk, params) {
    const rows = await readRowsFromFile(params.filePath, params.format);
    await importRowsForDe(sdk, {
        deKey: params.deKey,
        rows,
        api: params.api,
        mode: params.mode,
    });
}
