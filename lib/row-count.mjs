import { rowsetGetPath } from './import-routes.mjs';

/**
 * Fetch the total row count for a Data Extension without downloading all rows.
 * Uses a pagesize=1 request so only the `count` metadata is transferred.
 * Returns `null` if the count cannot be determined (e.g. permissions error).
 *
 * @param {{ rest: { get: (path: string) => Promise.<any> } }} sdk
 * @param {string} deKey - DE external key
 * @returns {Promise.<number|null>}
 */
export async function getDeRowCount(sdk, deKey) {
    try {
        const result = await sdk.rest.get(`${rowsetGetPath(deKey)}?$page=1&$pagesize=1`);
        return result?.count ?? 0;
    } catch (ex) {
        console.error(`Could not retrieve row count for DE "${deKey}": ${ex.message}`);
        return null;
    }
}
