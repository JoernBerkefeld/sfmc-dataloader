import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import SDK from 'sfmc-sdk';
import { resolveCredentialAndMid, buildSdkAuthObject } from './config.mjs';
import { fetchAllRowObjects, serializeRows } from './export-de.mjs';
import { exportDataExtensionToFile } from './export-de.mjs';
import { importRowsForDe } from './import-de.mjs';
import { readRowsFromFile } from './read-rows.mjs';
import { clearDataExtensionRows } from './clear-de.mjs';
import { confirmClearBeforeImport } from './confirm-clear.mjs';
import { dataDirectoryForBu } from './paths.mjs';
import { buildExportBasename, filesystemSafeTimestamp, parseExportBasename } from './filename.mjs';

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
 * Two source modes are supported:
 *
 * **API mode** (default): rows are fetched live from the source BU via the
 * SFMC REST API.  Requires `sourceCred`, `sourceBu`, and `deKeys`.
 *
 * **File mode**: rows are read from local export files (e.g. previously
 * created by `mcdata export`).  Requires `filePaths`; `deKeys` and
 * `sourceCred`/`sourceBu` must be omitted.  The DE customer key is derived
 * from each filename via `parseExportBasename`.
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
 * @param {string} [params.sourceCred] - API mode only
 * @param {string} [params.sourceBu]   - API mode only
 * @param {string[]} [params.deKeys]   - API mode only
 * @param {string[]} [params.filePaths] - File mode only; mutually exclusive with sourceCred/sourceBu/deKeys
 * @param {CredBuTarget[]} params.targets
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
        targets,
        format,
        api,
        mode,
        clearBeforeImport,
        acceptRiskFlag,
        isTTY,
    } = params;
    const stdin = params.stdin;
    const stdout = params.stdout;

    // Determine source mode
    const filePaths = params.filePaths ?? null;
    const isFileBased = filePaths !== null && filePaths.length > 0;

    // Derive DE keys: from explicit list (API mode) or from filenames (file mode)
    const deKeys = isFileBased
        ? filePaths.map((fp) => parseExportBasename(path.basename(fp)).customerKey)
        : (params.deKeys ?? []);

    // Build a lookup map from deKey → filePath for file mode
    /** @type {Map<string, string>} */
    const fileByDeKey = new Map();
    if (isFileBased) {
        for (const fp of filePaths) {
            fileByDeKey.set(parseExportBasename(path.basename(fp)).customerKey, fp);
        }
    }

    // Validate all target BU configurations upfront
    for (const { credential, bu } of targets) {
        resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
    }

    // Connect to source BU (API mode only)
    let srcSdk = null;
    if (!isFileBased) {
        const { mid: srcMid, authCred: srcAuth } = resolveCredentialAndMid(
            mcdevrc, mcdevAuth, params.sourceCred, params.sourceBu
        );
        srcSdk = new SDK(buildSdkAuthObject(srcAuth, srcMid), { requestAttempts: 3 });
    }

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

    // Load rows once per DE then fan out to every target
    for (const deKey of deKeys) {
        const rows = isFileBased
            ? await readRowsFromFile(fileByDeKey.get(deKey), format)
            : await fetchAllRowObjects(srcSdk, deKey);

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
            const snapshotPath = path.join(dir, basename);
            await fs.writeFile(snapshotPath, serializeRows(rows, format, false), 'utf8');
            console.error(`Download stored: ${snapshotPath}`);

            await importRowsForDe(tgtSdk, { deKey, rows, api, mode });
            console.error(`Imported -> ${credential}/${bu} DE ${deKey}`);
        }
    }
}
