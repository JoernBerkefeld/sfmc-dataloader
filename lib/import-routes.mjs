/**
 * REST paths for Data Extension row writes (relative to REST base URL).
 * Confirm against current Salesforce reference hubs when adjusting behavior.
 */

/**
 * @param {string} deKey
 * @returns {string} path starting with /
 */
export function rowsetGetPath(deKey) {
    return `/data/v1/customobjectdata/key/${encodeURIComponent(deKey)}/rowset`;
}

/**
 * Async bulk upsert (default for `--api async`).
 * @param {string} deKey
 * @returns {string}
 */
export function asyncUpsertPath(deKey) {
    return `/data/v1/async/dataextensions/key:${encodeURIComponent(deKey)}/rows`;
}

/**
 * Synchronous upsert row set.
 * @param {string} deKey
 * @returns {string}
 */
export function syncUpsertPath(deKey) {
    return `/data/v1/customobjectdata/key/${encodeURIComponent(deKey)}/rows`;
}

/**
 * Synchronous insert row set (POST).
 * @param {string} deKey
 * @returns {string}
 */
export function syncInsertPath(deKey) {
    return `/data/v1/customobjectdata/key/${encodeURIComponent(deKey)}/rows`;
}

/**
 * @param {'async'|'sync'} api
 * @param {'upsert'|'insert'|'update'} mode
 * @returns {{ method: 'PUT'|'POST', path: (de: string) => string }}
 */
export function resolveImportRoute(api, mode) {
    if (api === 'async') {
        if (mode !== 'upsert') {
            throw new Error(
                `Import mode "${mode}" is not supported with --api async (use --api sync or --mode upsert).`
            );
        }
        return { method: 'PUT', path: asyncUpsertPath };
    }
    if (api === 'sync') {
        if (mode === 'upsert') {
            return { method: 'PUT', path: syncUpsertPath };
        }
        if (mode === 'insert') {
            return { method: 'POST', path: syncInsertPath };
        }
        if (mode === 'update') {
            return { method: 'PUT', path: syncUpsertPath };
        }
    }
    throw new Error(`Unsupported --api / --mode combination: ${api} + ${mode}`);
}
