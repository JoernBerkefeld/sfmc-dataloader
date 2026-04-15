import { RestError } from 'sfmc-sdk/util';
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
 * @returns {Promise.<{ count: number, requestIds: (string|null)[] }>}
 */
export async function importRowsForDe(sdk, params) {
    const { deKey, rows, mode } = params;
    const route = resolveImportRoute(mode);
    const chunks = chunkItemsForPayload(rows);
    const requestIds = [];
    for (const chunk of chunks) {
        const p = route.path(deKey);
        const body = { items: chunk };
        let resp;
        try {
            resp = await withRetry429(() =>
                route.method === 'PUT' ? sdk.rest.put(p, body) : sdk.rest.post(p, body),
            );
        } catch (ex) {
            const msgs =
                ex instanceof RestError &&
                ex.response?.status === 400 &&
                Array.isArray(ex.response?.data?.resultMessages) &&
                ex.response.data.resultMessages.length > 0
                    ? ex.response.data.resultMessages
                    : null;
            if (msgs) {
                const summary = msgs.map((m) => m.message ?? String(m)).join('; ');
                throw new Error(`Import failed for DE "${deKey}" (HTTP 400): ${summary}`, {
                    cause: ex,
                });
            }
            throw ex;
        }
        requestIds.push(resp?.requestId ?? null);
    }
    return { count: rows.length, requestIds };
}

/**
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {object} params
 * @param {string} params.filePath
 * @param {string} params.deKey - target DE customer key for API
 * @param {'csv'|'tsv'|'json'} [params.format] - optional; auto-detected from file extension if omitted
 * @param {'upsert'|'insert'} params.mode
 * @returns {Promise.<{ count: number, requestIds: (string|null)[] }>}
 */
export async function importFromFile(sdk, params) {
    const format = params.format || formatFromExtension(params.filePath);
    if (!format) {
        throw new Error(
            `Cannot determine format for file: ${params.filePath}. Use .csv, .tsv, or .json extension.`,
        );
    }
    const rows = await readRowsFromFile(params.filePath, format);
    if (rows.length === 0) {
        throw new Error(
            `Import file contains no data rows: "${params.filePath}". ` +
                `The file may be empty, contain only a BOM, or contain only a header row. ` +
                `Export the DE first to obtain a template with column names, then add rows.`,
        );
    }
    return importRowsForDe(sdk, {
        deKey: params.deKey,
        rows,
        mode: params.mode,
    });
}
