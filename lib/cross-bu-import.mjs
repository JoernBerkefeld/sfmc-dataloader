import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import SDK from 'sfmc-sdk';
import { resolveCredentialAndMid, buildSdkAuthObject } from './config.mjs';
import { fetchAllRowObjects, serializeRows } from './export-de.mjs';
import { exportDataExtensionToFile } from './export-de.mjs';
import { importRowsForDe } from './import-de.mjs';
import { clearDataExtensionRows } from './clear-de.mjs';
import { confirmClearBeforeImport } from './confirm-clear.mjs';
import { dataDirectoryForBu } from './paths.mjs';
import { buildExportBasename, filesystemSafeTimestamp } from './filename.mjs';

/**
 * @typedef {{ credential: string, bu: string }} CredBuTarget
 */

/**
 * In TTY mode, asks the user whether to export target DE data as backup
 * before importing.  Returns true when the user answers YES.
 *
 * @param {object} opts
 * @param {CredBuTarget[]} opts.targets
 * @param {string[]} opts.deKeys
 * @param {NodeJS.ReadableStream} [opts.stdin]
 * @param {NodeJS.WritableStream} [opts.stdout]
 * @returns {Promise<boolean>}
 */
async function offerPreExportBackup({ targets, deKeys, stdin: stdinStream, stdout: stdoutStream }) {
    const stdinSrc = stdinStream ?? input;
    const stdoutSrc = stdoutStream ?? output;
    const targetList = targets.map(({ credential, bu }) => `${credential}/${bu}`).join(', ');
    const msg =
        '\nBefore importing, would you like to export the current data from target BU(s) as a backup?\n' +
        'This creates timestamped files that will not be overwritten by the following import.\n\n' +
        `  Target(s): ${targetList}\n` +
        `  Data Extensions: ${deKeys.join(', ')}\n\n` +
        'Type YES to export first, or press Enter to skip: ';
    stdoutSrc.write(msg);
    const rl = readline.createInterface({ input: stdinSrc, output: stdoutSrc });
    try {
        const line = await rl.question('');
        return line.trim() === 'YES';
    } finally {
        rl.close();
    }
}

/**
 * Imports Data Extension rows from a single source BU into one or more target
 * BUs.
 *
 * Before the import each target BU:
 * 1. Optionally exports its current DE data as a timestamped backup (TTY only).
 * 2. Optionally clears all existing rows (with danger warning covering every target).
 * 3. Receives the source rows written to a timestamped "download" file in its
 *    own `./data/<credential>/<bu>/` directory (mirroring mcdev retrieve).
 * 4. Has the rows imported via the REST API.
 *
 * @param {object} params
 * @param {string} params.projectRoot
 * @param {import('./config.mjs').Mcdevrc} params.mcdevrc
 * @param {Record<string, import('./config.mjs').AuthCredential>} params.mcdevAuth
 * @param {string} params.sourceCred
 * @param {string} params.sourceBu
 * @param {CredBuTarget[]} params.targets
 * @param {string[]} params.deKeys
 * @param {'csv'|'tsv'|'json'} params.format
 * @param {'async'|'sync'} params.api
 * @param {'upsert'|'insert'|'update'} params.mode
 * @param {boolean} params.clearBeforeImport
 * @param {boolean} params.acceptRiskFlag
 * @param {boolean} params.isTTY
 * @param {NodeJS.ReadableStream} [params.stdin]  Override for testing
 * @param {NodeJS.WritableStream} [params.stdout] Override for testing
 * @returns {Promise<void>}
 */
export async function crossBuImport(params) {
    const {
        projectRoot,
        mcdevrc,
        mcdevAuth,
        sourceCred,
        sourceBu,
        targets,
        deKeys,
        format,
        api,
        mode,
        clearBeforeImport,
        acceptRiskFlag,
        isTTY,
    } = params;
    const stdin = params.stdin;
    const stdout = params.stdout;

    // Validate all BU configurations upfront before making any API calls
    const { mid: srcMid, authCred: srcAuth } = resolveCredentialAndMid(mcdevrc, mcdevAuth, sourceCred, sourceBu);
    for (const { credential, bu } of targets) {
        resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
    }

    // Connect to source BU
    const srcSdk = new SDK(buildSdkAuthObject(srcAuth, srcMid), { requestAttempts: 3 });

    // Optional pre-import backup of target BU data
    if (isTTY) {
        const doBackup = await offerPreExportBackup({ targets, deKeys, stdin, stdout });
        if (doBackup) {
            for (const { credential, bu } of targets) {
                const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
                const tgtSdk = new SDK(buildSdkAuthObject(authCred, mid), { requestAttempts: 3 });
                for (const deKey of deKeys) {
                    const outPath = await exportDataExtensionToFile(tgtSdk, {
                        projectRoot,
                        credentialName: credential,
                        buName: bu,
                        deKey,
                        format,
                    });
                    console.error(`Backup export: ${outPath}`);
                }
            }
        }
    }

    // Single up-front clear confirmation covering all targets + all DE keys
    if (clearBeforeImport) {
        await confirmClearBeforeImport({ deKeys, targets, acceptRiskFlag, isTTY, stdin, stdout });
    }

    // Fetch rows once per DE from the source, then fan out to every target
    for (const deKey of deKeys) {
        const rows = await fetchAllRowObjects(srcSdk, deKey);

        // Clear targets before import (rows already confirmed above)
        if (clearBeforeImport) {
            for (const { credential, bu } of targets) {
                const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
                const tgtSdk = new SDK(buildSdkAuthObject(authCred, mid), { requestAttempts: 3 });
                await clearDataExtensionRows(tgtSdk.soap, deKey);
            }
        }

        for (const { credential, bu } of targets) {
            const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
            const tgtSdk = new SDK(buildSdkAuthObject(authCred, mid), { requestAttempts: 3 });

            // Write a timestamped "download" file in the target BU's data directory.
            // This mirrors what mcdev retrieve / mcdata export produces, giving a
            // traceable snapshot of exactly what was imported.
            const dir = dataDirectoryForBu(projectRoot, credential, bu);
            await fs.mkdir(dir, { recursive: true });
            const ts = filesystemSafeTimestamp();
            const basename = buildExportBasename(deKey, ts, format);
            const filePath = path.join(dir, basename);
            await fs.writeFile(filePath, serializeRows(rows, format, false), 'utf8');
            console.error(`Download stored: ${filePath}`);

            await importRowsForDe(tgtSdk, { deKey, rows, api, mode });
            console.error(`Imported -> ${credential}/${bu} DE ${deKey}`);
        }
    }
}
