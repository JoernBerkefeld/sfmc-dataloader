import { createReadStream, createWriteStream } from 'node:fs';
import fs from 'node:fs/promises';
import path from 'node:path';
import { finished, pipeline } from 'node:stream/promises';
import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import SDK from 'sfmc-sdk';
import { resolveCredentialAndMid, buildSdkAuthObject, buildSdkOptions } from './config.mjs';
import {
    fetchAllRowObjects,
    fetchDataExtensionFieldNames,
    serializeRows,
    exportDataExtensionToFile,
} from './export-de.mjs';
import { MAX_OBJECTS_PER_BATCH } from './batch.mjs';
import { formatFromExtension, resolveImportSet } from './file-resolve.mjs';
import {
    assertNonEmptyImportRowCount,
    importRowsForDe,
    importRowsStreamingForDe,
    warnIfImportCountUnexpected,
} from './import-de.mjs';
import { pollAsyncImportCompletion } from './async-status.mjs';
import {
    countDataRowsFromImportPaths,
    readRowsFromImportPaths,
    streamRowsFromImportPaths,
} from './read-rows.mjs';
import { clearDataExtensionRows } from './clear-de.mjs';
import { confirmClearBeforeImport } from './confirm-clear.mjs';
import { dataDirectoryForBu } from './paths.mjs';
import { buildExportBasename, filesystemSafeTimestamp, parseExportBasename } from './filename.mjs';
import { getDeRowCount } from './row-count.mjs';
import { log } from './log.mjs';

/**
 * Concatenates source files into a single snapshot on disk (streaming; bounded memory).
 *
 * @param {string} destinationPath
 * @param {string[]} sourcePaths
 * @returns {Promise.<void>}
 */
async function concatenateFilesToPath(destinationPath, sourcePaths) {
    const out = createWriteStream(destinationPath);
    for (const source of sourcePaths) {
        await pipeline(createReadStream(source), out, { end: false });
    }
    out.end();
    await finished(out);
}

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
    const stdinSource = stdinStream ?? input;
    const stdoutSource = stdoutStream ?? output;
    const targetList = targets.map(({ credential, bu }) => `${credential}/${bu}`).join(', ');
    const message =
        '\nBefore importing, would you like to export the current data from target BU(s) as a backup?\n' +
        'This creates timestamped files that will not be overwritten by the following import.\n\n' +
        `  Target(s): ${targetList}\n` +
        `  Data Extensions: ${deKeys.join(', ')}\n\n` +
        'Type YES to export first, or press Enter to skip: ';
    stdoutSource.write(message);
    const rl = readline.createInterface({ input: stdinSource, output: stdoutSource });
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
 * @param {object} parameters
 * @param {string} parameters.projectRoot
 * @param {import('./config.mjs').Mcdevrc} parameters.mcdevrc
 * @param {Record<string, import('./config.mjs').AuthCredential>} parameters.mcdevAuth
 * @param {string} [parameters.sourceCred] - API mode only
 * @param {string} [parameters.sourceBu] - API mode only
 * @param {string[]} [parameters.deKeys] - API mode only
 * @param {string[]} [parameters.filePaths] - File mode only; mutually exclusive with sourceCred/sourceBu/deKeys
 * @param {CredBuTarget[]} parameters.targets
 * @param {'csv'|'tsv'|'json'} parameters.format
 * @param {'upsert'|'insert'} parameters.mode
 * @param {boolean} [parameters.backupBeforeImport] - true=always backup, false=never backup, undefined=TTY prompt
 * @param {boolean} parameters.clearBeforeImport
 * @param {boolean} parameters.acceptRiskFlag
 * @param {boolean} parameters.isTTY
 * @param {boolean} [parameters.useGit] - accepted for API compatibility but ignored; snapshot files always use a timestamped name
 * @param {import('./config.mjs').DebugLogger|null} [parameters.logger] - debug logger for API requests/responses
 * @param {NodeJS.ReadableStream} [parameters.stdin] Override for testing
 * @param {NodeJS.WritableStream} [parameters.stdout] Override for testing
 * @returns {Promise.<boolean>} true if at least one import job returned an error
 */
export async function crossBuImport(parameters) {
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
    } = parameters;
    const stdin = parameters.stdin;
    const stdout = parameters.stdout;

    // Determine source mode
    const filePaths = parameters.filePaths ?? null;
    const isFileBased = filePaths !== null && filePaths.length > 0;

    // Derive DE keys: from explicit list (API mode) or from filenames (file mode, first-seen order)
    /**
     * @type {string[]}
     */
    const deKeys = isFileBased
        ? (() => {
              /**
               * @type {string[]}
               */
              const keys = [];
              const seen = new Set();
              for (const fp of filePaths) {
                  const k = parseExportBasename(path.basename(fp)).customerKey;
                  if (!seen.has(k)) {
                      seen.add(k);
                      keys.push(k);
                  }
              }
              return keys;
          })()
        : (parameters.deKeys ?? []);

    // Validate all target BU configurations upfront
    for (const { credential, bu } of targets) {
        resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
    }

    // Connect to source BU (API mode only)
    let sourceSdk = null;
    if (!isFileBased) {
        const { mid: sourceMid, authCred: sourceAuth } = resolveCredentialAndMid(
            mcdevrc,
            mcdevAuth,
            parameters.sourceCred,
            parameters.sourceBu,
        );
        sourceSdk = new SDK(buildSdkAuthObject(sourceAuth, sourceMid), buildSdkOptions(logger));
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
                const { paths: outPaths, rowCount } = await exportDataExtensionToFile(tgtSdk, {
                    projectRoot,
                    credentialName: credential,
                    buName: bu,
                    deKey,
                    format,
                    useGit: false,
                });
                const label = outPaths.map((p) => `"${path.resolve(p)}"`).join(', ');
                log.info(`Backup export: ${label} (${rowCount} rows)`);
            }
        }
    }

    // Single up-front clear confirmation covering all targets + all DE keys
    if (clearBeforeImport) {
        await confirmClearBeforeImport({ deKeys, targets, acceptRiskFlag, isTTY, stdin, stdout });
    }

    let hasError = false;

    // Load rows once per DE then fan out to every target (CSV/TSV file mode streams to avoid OOM)
    for (const deKey of deKeys) {
        /**
         * @type {object[]|null}
         */
        let rows = null;
        /**
         * @type {string[]|null}
         */
        let streamingImportPaths = null;
        /**
         * @type {'csv'|'tsv'|'json'|null}
         */
        let streamingDetectedFormat = null;

        if (isFileBased) {
            const groupPaths = filePaths.filter(
                (fp) => parseExportBasename(path.basename(fp)).customerKey === deKey,
            );
            const { paths: importPaths } = await resolveImportSet(groupPaths);
            if (importPaths.length === 0) {
                throw new Error(`No resolvable import files for DE "${deKey}".`);
            }
            const detectedFormat = formatFromExtension(importPaths[0]);
            if (!detectedFormat) {
                throw new Error(
                    `Cannot determine format for file: ${importPaths[0]}. Use .csv, .tsv, or .json extension.`,
                );
            }
            if (detectedFormat === 'csv' || detectedFormat === 'tsv') {
                streamingImportPaths = importPaths;
                streamingDetectedFormat = detectedFormat;
            } else {
                rows = await readRowsFromImportPaths(importPaths, detectedFormat);
                if (rows.length === 0) {
                    throw new Error(
                        `Import files contain no data rows for DE "${deKey}". ` +
                            `The files may be empty, contain only a BOM, or contain only a header row.`,
                    );
                }
            }
        } else {
            rows = await fetchAllRowObjects(sourceSdk, deKey);
        }

        let snapshotColumns = [];
        if (format !== 'json' && rows && rows.length === 0) {
            try {
                snapshotColumns = await fetchDataExtensionFieldNames(sourceSdk.soap, deKey);
            } catch (ex) {
                log.warn(
                    `Warning: could not retrieve field names for empty DE "${deKey}" (snapshot): ${ex.message}`,
                );
            }
        }

        for (const { credential, bu } of targets) {
            const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
            const tgtSdk = new SDK(buildSdkAuthObject(authCred, mid), buildSdkOptions(logger));

            const countBefore = await getDeRowCount(tgtSdk, deKey);
            log.info(
                `Row count before import: ${countBefore ?? '(unavailable)'} (${credential}/${bu} DE "${deKey}")`,
            );

            // Clear target before import (already confirmed above); skip if DE is empty
            let isClearedTargetDe = false;
            if (clearBeforeImport) {
                if (countBefore === 0) {
                    log.info(
                        `Skipping clear-data for ${credential}/${bu} DE "${deKey}" — DE is already empty.`,
                    );
                } else {
                    await clearDataExtensionRows(tgtSdk.soap, deKey);
                    isClearedTargetDe = true;
                    log.warn(`Cleared data: ${credential}/${bu} DE "${deKey}"`);
                }
            }

            // Write a snapshot file in the target BU's data directory.
            const directory = dataDirectoryForBu(projectRoot, credential, bu);
            await fs.mkdir(directory, { recursive: true });
            const ts = filesystemSafeTimestamp();
            const basename = buildExportBasename(deKey, ts, format, false);
            const snapshotPath = path.join(directory, basename);

            /**
             * @type {{ count: number, requestIds: (string|null)[] }}
             */
            let importResult;
            if (streamingImportPaths && streamingDetectedFormat) {
                await concatenateFilesToPath(snapshotPath, streamingImportPaths);
                log.info(`Download stored: "${path.resolve(snapshotPath)}"`);
                const rowCount = await countDataRowsFromImportPaths(
                    streamingImportPaths,
                    streamingDetectedFormat,
                );
                assertNonEmptyImportRowCount(rowCount, streamingImportPaths.join(', '));
                const totalMemoryBatches = Math.max(1, Math.ceil(rowCount / MAX_OBJECTS_PER_BATCH));
                const rowSource = streamRowsFromImportPaths(
                    streamingImportPaths,
                    streamingDetectedFormat,
                );
                importResult = await importRowsStreamingForDe(tgtSdk, {
                    deKey,
                    rowSource,
                    mode,
                    totalMemoryBatches,
                });
            } else {
                await fs.writeFile(
                    snapshotPath,
                    serializeRows(/** @type {object[]} */ (rows), format, false, snapshotColumns),
                    'utf8',
                );
                log.info(
                    `Download stored: "${path.resolve(snapshotPath)}" (${/** @type {object[]} */ (rows).length} rows)`,
                );
                importResult = await importRowsForDe(tgtSdk, {
                    deKey,
                    rows: /** @type {object[]} */ (rows),
                    mode,
                });
            }

            const { count: imported, requestIds } = importResult;
            log.info(`Imported: ${credential}/${bu} DE ${deKey} (${imported} rows)`);

            const importHadError = await pollAsyncImportCompletion(tgtSdk, requestIds);
            if (importHadError) {
                hasError = true;
            }

            const countAfter = await getDeRowCount(tgtSdk, deKey);
            log.info(
                `Row count after import: ${countAfter ?? '(unavailable)'} (${credential}/${bu} DE "${deKey}")`,
            );
            warnIfImportCountUnexpected({
                countBefore,
                cleared: isClearedTargetDe,
                countAfter,
                imported,
                mode,
                label: `${credential}/${bu} DE "${deKey}"`,
            });
        }
    }

    return hasError;
}
