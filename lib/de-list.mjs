import SDK from 'sfmc-sdk';
import {
    loadProjectConfig,
    resolveCredentialAndMid,
    buildSdkAuthObject,
    buildSdkOptions,
} from './config.mjs';

/**
 * Maps a SOAP retrieveBulk result for DataExtension into sorted `{ name, key }` rows.
 * Exported for unit tests; not part of the stable public API contract beyond testing.
 *
 * @param {object|null|undefined} bulkResult - `sdk.soap.retrieveBulk` response
 * @returns {{ name: string, key: string }[]}
 */
export function normalizeDeListFromBulkResult(bulkResult) {
    const rows = bulkResult?.Results;
    if (!Array.isArray(rows) || rows.length === 0) {
        return [];
    }

    const items = rows
        .map((row) => ({
            name: String(row.Name ?? ''),
            key: String(row.CustomerKey ?? ''),
        }))
        .filter((item) => item.key.length > 0);

    items.sort((a, b) => a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }));
    return items;
}

/**
 * Retrieves all Data Extension `Name` and `CustomerKey` values for a credential/BU via SOAP
 * (`retrieveBulk` handles pagination). For programmatic use (e.g. VS Code extension cache), not the CLI.
 *
 * @param {string} projectRoot - Absolute path to project root (mcdev or mcdata config pair)
 * @param {string} credential - Credential name from config
 * @param {string} bu - Business unit key from config
 * @returns {Promise.<{ name: string, key: string }[]>}
 */
export async function fetchDeList(projectRoot, credential, bu) {
    const { mcdevrc, mcdevAuth } = loadProjectConfig(projectRoot);
    const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
    const sdk = new SDK(buildSdkAuthObject(authCred, mid), buildSdkOptions());

    let bulkResult;
    try {
        bulkResult = await sdk.soap.retrieveBulk('DataExtension', ['Name', 'CustomerKey'], {});
    } catch (ex) {
        const message = ex instanceof Error ? ex.message : String(ex);
        throw new Error(
            `Could not retrieve Data Extensions — check credentials and BU. Original error: ${message}`,
            { cause: ex },
        );
    }

    return normalizeDeListFromBulkResult(bulkResult);
}
