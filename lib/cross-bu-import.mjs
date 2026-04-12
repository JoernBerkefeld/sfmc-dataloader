import fs from 'node:fs/promises';
import path from 'node:path';
import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import SDK from 'sfmc-sdk';
import { resolveCredentialAndMid, buildSdkAuthObject, buildSdkOptions } from './config.mjs';
import { fetchAllRowObjects, serializeRows, exportDataExtensionToFile } from './export-de.mjs';
import { formatFromExtension } from './file-resolve.mjs';
import { importRowsForDe } from './import-de.mjs';
import { readRowsFromFile } from './read-rows.mjs';
import { clearDataExtensionRows } from './clear-de.mjs';
import { confirmClearBeforeImport } from './confirm-clear.mjs';
import { dataDirectoryForBu, projectRelativePosix } from './paths.mjs';
import { buildExportBasename, filesystemSafeTimestamp, parseExportBasename } from './filename.mjs';
import { getDeRowCount } from './row-count.mjs';

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
 * @returns {Promise.<boolean>}
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
 * @param {object} params
 * @param {string} params.projectRoot
 * @param {import('./config.mjs').Mcdevrc} params.mcdevrc
 * @param {Record<string, import('./config.mjs').AuthCredential>} params.mcdevAuth
 * @param {string} [params.sourceCred] - API mode only
 * @param {string} [params.sourceBu] - API mode only
 * @param {string[]} [params.deKeys] - API mode only
 * @param {string[]} [params.filePaths] - File mode only; mutually exclusive with sourceCred/sourceBu/deKeys
 * @param {CredBuTarget[]} params.targets
 * @param {'csv'|'tsv'|'json'} params.format
 * @param {'upsert'|'insert'} params.mode
 * @param {boolean} [params.backupBeforeImport] - true=always backup, false=never backup, undefined=TTY prompt
 * @param {boolean} params.clearBeforeImport
 * @param {boolean} params.acceptRiskFlag
 * @param {boolean} params.isTTY
 * @param {boolean} [params.useGit] - accepted for API compatibility but ignored; snapshot files always use a timestamped name
 * @param {import('./config.mjs').DebugLogger|null} [params.logger] - debug logger for API requests/responses
 * @param {NodeJS.ReadableStream} [params.stdin] Override for testing
 * @param {NodeJS.WritableStream} [params.stdout] Override for testing
 * @returns {Promise.<void>}
 */
export async function crossBuImport(params) {
    const {
        projectRoot,
        mcdevrc,
        mcdevAuth,
        targets,
        format,
        mode,
        backupBeforeImport,
        clearBeforeImport,
        acceptRiskFlag,
        isTTY,
        logger = null,
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
            mcdevrc,
            mcdevAuth,
            params.sourceCred,
            params.sourceBu,
        );
        srcSdk = new SDK(buildSdkAuthObject(srcAuth, srcMid), buildSdkOptions(logger));
    }

    // Optional pre-import backup of target BU data
    const shouldBackup =
        backupBeforeImport === true
            ? true
            : backupBeforeImport === false
              ? false
              : isTTY
                ? await offerPreExportBackup({ targets, deKeys, stdin, stdout })
                : false;
    if (shouldBackup) {
        for (const { credential, bu } of targets) {
            const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
            const tgtSdk = new SDK(buildSdkAuthObject(authCred, mid), buildSdkOptions(logger));
            for (const deKey of deKeys) {
                const { path: outPath, rowCount } = await exportDataExtensionToFile(tgtSdk, {
                    projectRoot,
                    credentialName: credential,
                    buName: bu,
                    deKey,
                    format,
                    useGit: false,
                });
                const rel = projectRelativePosix(projectRoot, outPath);
                console.error(`Backup export: ${rel} (${rowCount} rows)`);
            }
        }
    }

    // Single up-front clear confirmation covering all targets + all DE keys
    if (clearBeforeImport) {
        await confirmClearBeforeImport({ deKeys, targets, acceptRiskFlag, isTTY, stdin, stdout });
    }

    // Load rows once per DE then fan out to every target
    for (const deKey of deKeys) {
        let rows;
        if (isFileBased) {
            const filePath = fileByDeKey.get(deKey);
            const detectedFormat = formatFromExtension(filePath);
            if (!detectedFormat) {
                throw new Error(
                    `Cannot determine format for file: ${filePath}. Use .csv, .tsv, or .json extension.`,
                );
            }
            rows = await readRowsFromFile(filePath, detectedFormat);
        } else {
            rows = await fetchAllRowObjects(srcSdk, deKey);
        }

        for (const { credential, bu } of targets) {
            const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
            const tgtSdk = new SDK(buildSdkAuthObject(authCred, mid), buildSdkOptions(logger));

            const countBefore = await getDeRowCount(tgtSdk, deKey);
            console.error(
                `Row count before import: ${countBefore ?? '(unavailable)'} (${credential}/${bu} DE "${deKey}")`,
            );

            // Clear target before import (already confirmed above); skip if DE is empty
            if (clearBeforeImport) {
                if (countBefore === 0) {
                    console.error(
                        `Skipping clear-data for ${credential}/${bu} DE "${deKey}" — DE is already empty.`,
                    );
                } else {
                    await clearDataExtensionRows(tgtSdk.soap, deKey);
                    console.warn(`Cleared data: ${credential}/${bu} DE "${deKey}"`);
                }
            }

            // Write a snapshot file in the target BU's data directory.
            const dir = dataDirectoryForBu(projectRoot, credential, bu);
            await fs.mkdir(dir, { recursive: true });
            const ts = filesystemSafeTimestamp();
            const basename = buildExportBasename(deKey, ts, format, false);
            const snapshotPath = path.join(dir, basename);
            await fs.writeFile(snapshotPath, serializeRows(rows, format, false), 'utf8');
            const snapRel = projectRelativePosix(projectRoot, snapshotPath);
            console.error(`Download stored: ${snapRel} (${rows.length} rows)`);

            const imported = await importRowsForDe(tgtSdk, { deKey, rows, mode });
            console.error(`Imported: ${credential}/${bu} DE ${deKey} (${imported} rows)`);

            const countAfter = await getDeRowCount(tgtSdk, deKey);
            console.error(
                `Row count after import: ${countAfter ?? '(unavailable)'} (${credential}/${bu} DE "${deKey}")`,
            );
            if (countAfter === null) {
                console.error(
                    `Could not verify import result for ${credential}/${bu} DE "${deKey}".`,
                );
            } else {
                const expected =
                    mode === 'insert' || countBefore === 0
                        ? (countBefore ?? 0) + imported
                        : imported;
                if (countAfter < expected) {
                    console.error(
                        `Import result for ${credential}/${bu} DE "${deKey}" looks unexpected: expected at least ${expected} rows, got ${countAfter}. Note: the async API may not have committed all rows yet.`,
                    );
                }
            }
        }
    }
}
