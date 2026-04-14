/**
 * REST paths for Data Extension row operations (relative to REST base URL).
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
 * Async bulk row writes: POST insert, PUT upsert (same URL).
 *
 * @param {string} deKey
 * @returns {string}
 */
export function asyncDataExtensionRowsPath(deKey) {
    return `/data/v1/async/dataextensions/key:${encodeURIComponent(deKey)}/rows`;
}

/**
 * Status endpoint for a previously submitted async request.
 *
 * @param {string} requestId
 * @returns {string}
 */
export function asyncRequestStatusPath(requestId) {
    return `/data/v1/async/${encodeURIComponent(requestId)}/status`;
}

/**
 * Results endpoint for a previously submitted async request.
 *
 * @param {string} requestId
 * @returns {string}
 */
export function asyncRequestResultsPath(requestId) {
    return `/data/v1/async/${encodeURIComponent(requestId)}/results`;
}

/**
 * @param {'upsert'|'insert'} mode
 * @returns {{ method: 'PUT'|'POST', path: (de: string) => string }}
 */
export function resolveImportRoute(mode) {
    if (mode === 'upsert') {
        return { method: 'PUT', path: asyncDataExtensionRowsPath };
    }
    if (mode === 'insert') {
        return { method: 'POST', path: asyncDataExtensionRowsPath };
    }
    throw new Error(`Unsupported import mode "${mode}" (use upsert or insert).`);
}
