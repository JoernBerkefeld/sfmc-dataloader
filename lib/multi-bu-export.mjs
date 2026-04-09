import SDK from 'sfmc-sdk';
import { resolveCredentialAndMid, buildSdkAuthObject } from './config.mjs';
import { exportDataExtensionToFile } from './export-de.mjs';
import { projectRelativePosix } from './paths.mjs';

/**
 * @typedef {{ credential: string, bu: string }} CredBuSource
 */

/**
 * Exports one or more Data Extensions from multiple source BUs in a single
 * pass.  Each source BU gets its own timestamped file per DE key under
 * `./data/<credential>/<bu>/`.
 *
 * @param {object} params
 * @param {string} params.projectRoot
 * @param {import('./config.mjs').Mcdevrc} params.mcdevrc
 * @param {Record<string, import('./config.mjs').AuthCredential>} params.mcdevAuth
 * @param {CredBuSource[]} params.sources
 * @param {string[]} params.deKeys
 * @param {'csv'|'tsv'|'json'} params.format
 * @param {boolean} [params.jsonPretty]
 * @param {boolean} [params.useGit]
 * @returns {Promise<string[]>} Paths of all written files
 */
export async function multiBuExport({ projectRoot, mcdevrc, mcdevAuth, sources, deKeys, format, jsonPretty = false, useGit = false }) {
    /** @type {string[]} */
    const exported = [];
    for (const { credential, bu } of sources) {
        const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
        const sdk = new SDK(buildSdkAuthObject(authCred, mid), { requestAttempts: 3 });
        for (const deKey of deKeys) {
            const { path: outPath, rowCount } = await exportDataExtensionToFile(sdk, {
                projectRoot,
                credentialName: credential,
                buName: bu,
                deKey,
                format,
                jsonPretty,
                useGit,
            });
            const rel = projectRelativePosix(projectRoot, outPath);
            console.error(`Exported: ${rel} (${rowCount} rows)`);
            exported.push(outPath);
        }
    }
    return exported;
}
