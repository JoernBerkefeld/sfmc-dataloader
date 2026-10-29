import { createWriteStream } from 'node:fs';
import os from 'node:os';
import { randomUUID } from 'node:crypto';
import { Readable } from 'node:stream';
import { stringify } from 'csv-stringify';
import fs from 'node:fs/promises';
import path from 'node:path';
import { pipeline } from 'node:stream/promises';
import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import SDK from 'sfmc-sdk';
import { resolveCredentialAndMid, buildSdkAuthObject, buildSdkOptions } from './config.mjs';
import { exportDataExtensionToFile, createBackupPathGuard } from './export-de.mjs';
import { formatFromExtension, resolveImportSet } from './file-resolve.mjs';
import {
    assertNonEmptyImportRowCount,
    importRowsStreamingForDe,
    warnIfImportCountUnexpected,
} from './import-de.mjs';
import { pollAsyncImportCompletion } from './async-status.mjs';
import { preflightImportPaths, streamRowsFromImportPaths } from './read-rows.mjs';
import { clearDataExtensionRows } from './clear-de.mjs';
import { confirmClearBeforeImport } from './confirm-clear.mjs';
import { dataDirectoryForBu } from './paths.mjs';
import {
    buildExportBasename,
    filesystemSafeTimestamp,
    resolveExplicitImportBasename,
} from './filename.mjs';
import { getDeRowCount } from './row-count.mjs';
import { log } from './log.mjs';

/**
 * Stage API downloads outside source discovery; retain the owned run for diagnostics.
 *
 * @returns {Promise.<string>}
 */
async function createSourceStage() {
    const temporaryDirectory = os.tmpdir();
    if (
        !path.isAbsolute(temporaryDirectory) ||
        path.resolve(temporaryDirectory) === path.parse(path.resolve(temporaryDirectory)).root
    ) {
        throw new Error('Source staging requires an absolute, non-root OS temp directory.');
    }
    const root = await fs.mkdtemp(path.join(temporaryDirectory, 'mcdata-cross-bu-'));
    await fs.writeFile(path.join(root, '.stage-owner'), randomUUID(), { flag: 'wx' });
    return root;
}

/**
 * Serialize logical records, not multipart bytes, into a format-correct snapshot.
 *
 * @param {string} destinationPath
 * @param {object} metadata
 * @param {'csv'|'tsv'|'json'} format
 * @returns {Promise.<void>}
 */
async function writeSourceSnapshot(destinationPath, metadata, format) {
    const rows = streamRowsFromImportPaths(metadata.paths, metadata.format, metadata);
    if (format === 'json') {
        // Match compact array serialization without collecting multipart rows or output bytes.
        const chunks = async function* () {
            yield '[';
            let separator = '';
            for await (const row of rows) {
                yield separator + JSON.stringify(row);
                separator = ',';
            }
            yield ']\n';
        };
        await pipeline(
            Readable.from(chunks()),
            createWriteStream(destinationPath, { flags: 'wx' }),
        );
        return;
    }
    await pipeline(
        Readable.from(rows),
        stringify({
            header: true,
            bom: true,
            quoted: format === 'csv',
            delimiter: format === 'tsv' ? '\t' : ',',
        }),
        createWriteStream(destinationPath, { flags: 'wx' }),
    );
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
 * from each filename via `resolveExplicitImportBasename`.
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
 * @param {boolean} [parameters.skipEmptyLines] - CSV/TSV empty-record skipping (default true)
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
                  const k = resolveExplicitImportBasename(path.basename(fp)).customerKey;
                  if (seen.has(k)) {
                      continue;
                  }

                  seen.add(k);
                  keys.push(k);
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

    // Freeze every selected set before backups can change filesystem discovery.
    const preparedSources = [];
    const stageRoot = isFileBased ? null : await createSourceStage();
    for (const deKey of deKeys) {
        let importPaths;
        if (isFileBased) {
            const groupPaths = filePaths.filter(
                (fp) => resolveExplicitImportBasename(path.basename(fp)).customerKey === deKey,
            );
            ({ paths: importPaths } = await resolveImportSet(groupPaths, {
                explicitSelection: true,
            }));
        } else {
            ({ paths: importPaths } = await exportDataExtensionToFile(sourceSdk, {
                projectRoot: stageRoot,
                credentialName: 'source',
                buName: 'staged',
                deKey,
                format,
                useGit: false,
            }));
        }
        const detectedFormat = formatFromExtension(importPaths[0]);
        if (!detectedFormat) {
            throw new Error(`Cannot determine source format for DE "${deKey}".`);
        }
        preparedSources.push({
            deKey,
            paths: Object.freeze([...importPaths]),
            format: detectedFormat,
        });
    }
    // Validate the complete invocation before any target writer, CLEAR or upload.
    for (const source of preparedSources) {
        source.metadata = await preflightImportPaths(source.paths, source.format, {
            skipEmptyLines: parameters.skipEmptyLines,
        });
        assertNonEmptyImportRowCount(source.metadata.recordCount, source.paths.join(', '));
        Object.freeze(source);
    }
    Object.freeze(preparedSources);
    const guardSources = await createBackupPathGuard(
        preparedSources.flatMap((source) => source.paths),
    );
    // Preserve all existing outputs, including backups generated earlier in this invocation.
    const beforeOpen = async (destinationPath) => {
        await guardSources(destinationPath);
        try {
            await fs.lstat(destinationPath);
        } catch (ex) {
            if (ex.code === 'ENOENT') {
                return;
            }
            throw ex;
        }
        throw new Error(
            `Output destination already exists; refusing to overwrite: "${destinationPath}".`,
        );
    };

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
                    beforeOpen,
                });
                const label = outPaths.map((p) => `"${path.resolve(p)}"`).join(', ');
                log.info(`Backup export: ${label} (${rowCount} rows)`);
            }
        }
    }

    // Finish all target snapshots before the first target mutation, including later DEs.
    const preparedJobs = [];
    for (const source of preparedSources) {
        for (const { credential, bu } of targets) {
            const directory = dataDirectoryForBu(projectRoot, credential, bu);
            await fs.mkdir(directory, { recursive: true });
            const snapshotPath = path.join(
                directory,
                buildExportBasename(source.deKey, filesystemSafeTimestamp(), format, false),
            );
            await beforeOpen(snapshotPath);
            await writeSourceSnapshot(snapshotPath, source.metadata, format);
            log.info(`Download stored: "${path.resolve(snapshotPath)}"`);
            preparedJobs.push(Object.freeze({ source, credential, bu, snapshotPath }));
        }
    }

    // Single up-front clear confirmation covering all targets + all DE keys
    if (clearBeforeImport) {
        await confirmClearBeforeImport({ deKeys, targets, acceptRiskFlag, isTTY, stdin, stdout });
    }

    let hasError = false;
    const acceptedJobs = [];

    // Consume only the frozen preparation descriptors; do not reselect or recount.
    for (const { source, credential, bu } of preparedJobs) {
        const { deKey, metadata } = source;
        const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
        const tgtSdk = new SDK(buildSdkAuthObject(authCred, mid), buildSdkOptions(logger));

        let clearStatus = 'not requested';
        let submission = { acceptedRows: 0, acceptedRequests: 0, requestIds: [] };
        try {
            const countBefore = await getDeRowCount(tgtSdk, deKey);
            log.info(
                `Row count before import: ${countBefore ?? '(unavailable)'} (${credential}/${bu} DE "${deKey}")`,
            );

            // Clear target before import (already confirmed above); skip if DE is empty
            let isClearedTargetDe = false;
            if (clearBeforeImport) {
                if (countBefore === 0) {
                    clearStatus = 'skipped (already empty)';
                    log.info(
                        `Skipping clear-data for ${credential}/${bu} DE "${deKey}" — DE is already empty.`,
                    );
                } else {
                    clearStatus = 'attempted; outcome unknown';
                    await clearDataExtensionRows(tgtSdk.soap, deKey);
                    clearStatus = 'succeeded';
                    isClearedTargetDe = true;
                    log.warn(`Cleared data: ${credential}/${bu} DE "${deKey}"`);
                }
            }

            const rowSource = streamRowsFromImportPaths(metadata.paths, metadata.format, metadata);
            const { count: imported, requestIds } = await importRowsStreamingForDe(tgtSdk, {
                deKey,
                rowSource,
                mode,
            });
            submission = {
                acceptedRows: imported,
                acceptedRequests: requestIds.length,
                requestIds: [...requestIds],
            };
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
            acceptedJobs.push(`${credential}/${bu} DE "${deKey}"`);
        } catch (ex) {
            const accepted = ex.importSubmission ?? submission;
            const error = new Error(
                `Cross-BU import interrupted for ${credential}/${bu} DE "${deKey}": ${ex.message ?? String(ex)}. ` +
                    `CLEAR status: ${clearStatus}. Earlier accepted target jobs: ${acceptedJobs.length}` +
                    `${acceptedJobs.length > 0 ? ` (${acceptedJobs.join(', ')})` : ''}. ` +
                    `Current job: ${accepted.acceptedRows} rows in ${accepted.acceptedRequests} requests accepted. ` +
                    'Async acceptance does not confirm completion; the failed operation may have reached the server. ' +
                    'No automatic retry or rollback.',
                { cause: ex },
            );
            error.importSubmission = { ...accepted, requestIds: [...accepted.requestIds] };
            throw error;
        }
    }

    return hasError;
}
