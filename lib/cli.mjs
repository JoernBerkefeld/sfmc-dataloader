import { readFileSync } from 'node:fs';
import { parseArgs } from 'node:util';
import process from 'node:process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import SDK from 'sfmc-sdk';
import {
    loadProjectConfig,
    parseCredBu,
    resolveCredentialAndMid,
    buildSdkAuthObject,
    buildSdkOptions,
} from './config.mjs';
import { dataDirectoryForBu } from './paths.mjs';
import { exportDataExtensionToFile } from './export-de.mjs';
import { findImportCandidates, formatFromExtension, resolveImportSet } from './file-resolve.mjs';
import { parseExportBasename } from './filename.mjs';
import { importRowsForDe } from './import-de.mjs';
import { pollAsyncImportCompletion } from './async-status.mjs';
import { clearDataExtensionRows } from './clear-de.mjs';
import { confirmClearBeforeImport } from './confirm-clear.mjs';
import { getDeRowCount } from './row-count.mjs';
import { multiBuExport } from './multi-bu-export.mjs';
import { crossBuImport } from './cross-bu-import.mjs';
import { initDebugLogger } from './debug-logger.mjs';
import { runMcdataInit } from './init-project.mjs';
import { readRowsFromImportPaths } from './read-rows.mjs';

/**
 * @param {string|undefined} raw
 * @param {string} flagName
 * @returns {number|undefined}
 */
function parseOptionalPositiveInt(raw, flagName) {
    if (raw === undefined || raw === '') {
        return;
    }
    const n = Number(raw);
    if (!Number.isFinite(n) || n < 1) {
        throw new Error(`Invalid ${flagName}: ${raw} (expect positive integer)`);
    }
    return Math.floor(n);
}

/** @returns {string} semver from this package's package.json */
function readCliPackageVersion() {
    const injected = globalThis.__sfmc_dataloader_version__;
    if (typeof injected === 'string' && injected.length > 0) {
        return injected;
    }
    const pkgPath = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'package.json');
    const pkg = JSON.parse(readFileSync(pkgPath, 'utf8'));
    return typeof pkg.version === 'string' ? pkg.version : '';
}

function printHelp() {
    console.log(`mcdata - SFMC Data Extension export/import

Usage:
  mcdata init [options]
  mcdata export <credential>/<bu> --de <key> [--de <key> ...] [options]
  mcdata export --from <cred>/<bu> [--from <cred>/<bu> ...] --de <key> [--de <key> ...] [options]
  mcdata import <credential>/<bu> (--de <key> ... | --file <path> ...) [options]
  mcdata import --from <cred>/<bu> --to <cred>/<bu> [--to <cred>/<bu> ...] --de <key> ... [options]
  mcdata import --to <cred>/<bu> [--to <cred>/<bu> ...] --file <path> [--file <path> ...] [options]

Options:
  --version               Print version and exit
  -p, --project <dir>     Project root (default: cwd)
  --format <csv|tsv|json> Export file format (default: csv); ignored for imports
  --json-pretty           Pretty-print JSON on export
  --git                   Stable filenames: <key>.mcdata.<ext> (no timestamp)
  --max-rows-per-file <n> Split exports into multiple files after N data rows (optional)
  --debug                 Write API requests/responses to ./logs/data/*.log

Init options:
  --credential <name>     Credential name (e.g. MyOrg)
  --client-id <id>        Installed package client ID
  --client-secret <sec>   Installed package client secret
  --auth-url <url>        Auth URL (e.g. https://<tenantsubdomain>.auth.marketingcloudapis.com/)
  --enterprise-id <mid>   Enterprise MID (parent account ID)
  -y, --yes               Overwrite existing .mcdatarc.json / .mcdata-auth.json without prompt

Import options:
  --mode <upsert|insert>  Row write mode (default: upsert; async REST bulk API)
  --backup-before-import  Export target DE data as a timestamped backup before import (no prompt)
  --no-backup-before-import  Skip the backup prompt even in interactive (TTY) sessions
  --clear-before-import   SOAP ClearData before import (destructive; see below)
  --i-accept-clear-data-risk  Non-interactive acknowledgement for --clear-before-import

Multi-BU options:
  --from <cred>/<bu>      Export: source BU (repeatable for multiple sources)
                          Import (API mode): single source BU (use with --to and --de)
  --to <cred>/<bu>        Import: target BU (repeatable for multiple targets)
                          Import (file mode): use with --file only (no --from needed)

Config files:
  .mcdevrc.json / .mcdev-auth.json       mcdev layout (wins when both pairs present)
  .mcdatarc.json / .mcdata-auth.json     standalone mcdata layout (created by mcdata init)

Notes:
  Exports are written under ./data/<credential>/<bu>/ using ".mcdata." in the filename.
  Import with --de resolves the latest matching export (csv/tsv/json), including multi-part partN files.
  Import with --file parses the DE key from the basename (.mcdata. format).
  Import format is auto-detected from file extension (.csv, .tsv, .json).
  Cross-BU import stores a download file in each target BU's data directory.

Clear data warning:
  --clear-before-import deletes ALL existing rows in the target DE(s) before upload.
  Interactive: type YES when prompted. CI: also pass --i-accept-clear-data-risk.
`);
}

/**
 * @param {string[]} argv
 * @returns {Promise.<number>} exit code
 */
export async function main(argv) {
    let values;
    let positionals;
    try {
        const parsed = parseArgs({
            args: argv.slice(2),
            allowPositionals: true,
            strict: true,
            options: {
                project: { type: 'string', short: 'p' },
                format: { type: 'string' },
                de: { type: 'string', multiple: true },
                file: { type: 'string', multiple: true },
                from: { type: 'string', multiple: true },
                to: { type: 'string', multiple: true },
                git: { type: 'boolean', default: false },
                mode: { type: 'string' },
                'backup-before-import': { type: 'boolean', default: false },
                'no-backup-before-import': { type: 'boolean', default: false },
                'clear-before-import': { type: 'boolean', default: false },
                'i-accept-clear-data-risk': { type: 'boolean', default: false },
                'json-pretty': { type: 'boolean', default: false },
                debug: { type: 'boolean', default: false },
                help: { type: 'boolean', short: 'h', default: false },
                version: { type: 'boolean', default: false },
                credential: { type: 'string' },
                'client-id': { type: 'string' },
                'client-secret': { type: 'string' },
                'auth-url': { type: 'string' },
                'enterprise-id': { type: 'string' },
                yes: { type: 'boolean', short: 'y', default: false },
                'max-rows-per-file': { type: 'string' },
            },
        });
        values = parsed.values;
        positionals = parsed.positionals;
    } catch (ex) {
        console.error(ex.message);
        printHelp();
        return 1;
    }

    if (values.help) {
        printHelp();
        return 0;
    }
    if (values.version) {
        console.log(readCliPackageVersion());
        return 0;
    }
    if (positionals.length === 0) {
        printHelp();
        return 1;
    }

    const sub = positionals[0];
    const credBuRaw = positionals[1]; // May be undefined when --from/--to flags are used

    const projectRoot = path.resolve(values.project ?? process.cwd());

    // ── init ─────────────────────────────────────────────────────────────────
    if (sub === 'init') {
        return runMcdataInit({
            projectRoot,
            isTTY: process.stdin.isTTY === true,
            credential: values.credential,
            clientId: values['client-id'],
            clientSecret: values['client-secret'],
            authUrl: values['auth-url'],
            enterpriseId: values['enterprise-id'],
            yes: values.yes,
        });
    }

    const fmt = values.format ?? 'csv';
    if (!['csv', 'tsv', 'json'].includes(fmt)) {
        console.error(`Invalid --format: ${fmt}`);
        return 1;
    }

    const backupBeforeImport = values['backup-before-import']
        ? true
        : values['no-backup-before-import']
          ? false
          : undefined;

    const useGit = values.git === true;
    const fromFlags = values.from ?? [];
    const toFlags = values.to ?? [];
    const hasFrom = fromFlags.length > 0;
    const hasTo = toFlags.length > 0;
    const hasPositional = !!credBuRaw;

    // Initialize debug logger if --debug flag is set
    /** @type {import('./debug-logger.mjs').DebugLogger|null} */
    const logger = values.debug
        ? initDebugLogger(projectRoot, readCliPackageVersion(), argv)
        : null;
    if (logger) {
        console.error(`Debug log: "${path.resolve(logger.logPath)}"`);
    }

    // ── export ──────────────────────────────────────────────────────────────
    if (sub === 'export') {
        if (hasTo) {
            console.error('--to is not valid for export. Did you mean import?');
            return 1;
        }

        const des = [values.de ?? []].flat();
        if (des.length === 0) {
            console.error('export requires at least one --de <customerKey>');
            return 1;
        }

        if (hasFrom && hasPositional) {
            console.error(
                'Cannot mix a positional <credential>/<bu> with --from. Use one or the other.',
            );
            return 1;
        }

        if (!hasFrom && !hasPositional) {
            console.error(
                'export requires either a positional <credential>/<bu> or at least one --from <cred>/<bu>.',
            );
            printHelp();
            return 1;
        }

        if (hasFrom) {
            let sources;
            try {
                sources = fromFlags.map(parseCredBu);
            } catch (ex) {
                console.error(ex.message);
                return 1;
            }
            const { mcdevrc, mcdevAuth } = loadProjectConfig(projectRoot);
            let maxRowsPerFile;
            try {
                maxRowsPerFile = parseOptionalPositiveInt(
                    values['max-rows-per-file'],
                    '--max-rows-per-file',
                );
            } catch (ex) {
                console.error(ex.message);
                return 1;
            }
            await multiBuExport({
                projectRoot,
                mcdevrc,
                mcdevAuth,
                sources,
                deKeys: des,
                format: /** @type {'csv'|'tsv'|'json'} */ (fmt),
                jsonPretty: values['json-pretty'],
                useGit,
                maxRowsPerFile,
                logger,
            });
            return 0;
        }

        let maxRowsPerFile;
        try {
            maxRowsPerFile = parseOptionalPositiveInt(
                values['max-rows-per-file'],
                '--max-rows-per-file',
            );
        } catch (ex) {
            console.error(ex.message);
            return 1;
        }

        const { credential, bu } = parseCredBu(credBuRaw);
        const { mcdevrc, mcdevAuth } = loadProjectConfig(projectRoot);
        const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
        const sdk = new SDK(buildSdkAuthObject(authCred, mid), buildSdkOptions(logger));
        for (const deKey of des) {
            const { paths: outPaths, rowCount } = await exportDataExtensionToFile(sdk, {
                projectRoot,
                credentialName: credential,
                buName: bu,
                deKey,
                format: /** @type {'csv'|'tsv'|'json'} */ (fmt),
                jsonPretty: values['json-pretty'],
                useGit,
                maxRowsPerFile,
            });
            const label = outPaths.map((p) => `"${path.resolve(p)}"`).join(', ');
            console.error(`Exported: ${label} (${rowCount} rows)`);
        }
        return 0;
    }

    // ── import ──────────────────────────────────────────────────────────────
    if (sub === 'import') {
        const mode = values.mode ?? 'upsert';
        if (!['upsert', 'insert'].includes(mode)) {
            console.error(`Invalid --mode: ${mode} (use upsert or insert)`);
            return 1;
        }

        const clear = values['clear-before-import'];
        const acceptRisk = values['i-accept-clear-data-risk'];

        // ── File-to-multi-BU import: --to + --file (no --from) ─────────────
        if (hasTo && !hasFrom && values.file?.length > 0) {
            if (hasPositional) {
                console.error(
                    'Cannot mix a positional <credential>/<bu> with --to/--file. Use one or the other.',
                );
                return 1;
            }
            if (values.de?.length > 0) {
                console.error(
                    'Cannot mix --de with --file in multi-target import. Use --file only.',
                );
                return 1;
            }
            const filePaths = values.file;
            let targets;
            try {
                targets = toFlags.map(parseCredBu);
            } catch (ex) {
                console.error(ex.message);
                return 1;
            }
            const { mcdevrc, mcdevAuth } = loadProjectConfig(projectRoot);
            const crossBuHadError = await crossBuImport({
                projectRoot,
                mcdevrc,
                mcdevAuth,
                filePaths,
                targets,
                format: /** @type {'csv'|'tsv'|'json'} */ (fmt),
                mode: /** @type {'upsert'|'insert'} */ (mode),
                backupBeforeImport,
                clearBeforeImport: clear,
                acceptRiskFlag: acceptRisk,
                isTTY: process.stdin.isTTY === true,
                useGit,
                logger,
            });
            return crossBuHadError ? 1 : 0;
        }

        // ── Cross-BU import (API mode): --from + --to + --de ────────────────
        if (hasFrom || hasTo) {
            if (hasPositional) {
                console.error(
                    'Cannot mix a positional <credential>/<bu> with --from/--to. Use one or the other.',
                );
                return 1;
            }
            if (!hasFrom) {
                console.error(
                    '--to requires --from <cred>/<bu> to specify the source Business Unit.',
                );
                return 1;
            }
            if (!hasTo) {
                console.error(
                    '--from requires at least one --to <cred>/<bu> to specify target Business Unit(s).',
                );
                return 1;
            }
            if (fromFlags.length > 1) {
                console.error(
                    'import accepts exactly one --from <cred>/<bu> (use multiple --to for multiple targets).',
                );
                return 1;
            }
            if (values.file?.length > 0) {
                console.error(
                    '--file cannot be combined with --from/--to/--de. For file-based multi-target import use --to + --file (without --from).',
                );
                return 1;
            }
            const deKeys = [values.de ?? []].flat();
            if (deKeys.length === 0) {
                console.error('Cross-BU import requires at least one --de <customerKey>.');
                return 1;
            }
            let sourceParsed;
            let targets;
            try {
                sourceParsed = parseCredBu(fromFlags[0]);
                targets = toFlags.map(parseCredBu);
            } catch (ex) {
                console.error(ex.message);
                return 1;
            }
            const { mcdevrc, mcdevAuth } = loadProjectConfig(projectRoot);
            const crossBuApiHadError = await crossBuImport({
                projectRoot,
                mcdevrc,
                mcdevAuth,
                sourceCred: sourceParsed.credential,
                sourceBu: sourceParsed.bu,
                targets,
                deKeys,
                format: /** @type {'csv'|'tsv'|'json'} */ (fmt),
                mode: /** @type {'upsert'|'insert'} */ (mode),
                backupBeforeImport,
                clearBeforeImport: clear,
                acceptRiskFlag: acceptRisk,
                isTTY: process.stdin.isTTY === true,
                useGit,
                logger,
            });
            return crossBuApiHadError ? 1 : 0;
        }

        // ── Single-BU import (original behavior) ────────────────────────────
        if (!hasPositional) {
            console.error(
                'import requires either a positional <credential>/<bu> or --from/--to flags.',
            );
            printHelp();
            return 1;
        }

        const hasDe = values.de?.length > 0;
        const hasFile = values.file?.length > 0;
        if (hasDe === hasFile) {
            console.error(
                'import requires exactly one of: repeated --de <key> OR repeated --file <path>',
            );
            return 1;
        }

        const { credential, bu } = parseCredBu(credBuRaw);
        const { mcdevrc, mcdevAuth } = loadProjectConfig(projectRoot);
        const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
        const sdk = new SDK(buildSdkAuthObject(authCred, mid), buildSdkOptions(logger));

        if (hasDe) {
            const deKeys = [values.de ?? []].flat();
            const dataDir = dataDirectoryForBu(projectRoot, credential, bu);
            if (backupBeforeImport === true) {
                for (const deKey of deKeys) {
                    const { paths: outPaths, rowCount } = await exportDataExtensionToFile(sdk, {
                        projectRoot,
                        credentialName: credential,
                        buName: bu,
                        deKey,
                        format: /** @type {'csv'|'tsv'|'json'} */ (fmt),
                        useGit: false,
                    });
                    const label = outPaths.map((p) => `"${path.resolve(p)}"`).join(', ');
                    console.error(`Backup export: ${label} (${rowCount} rows)`);
                }
            }
            if (clear) {
                await confirmClearBeforeImport({
                    deKeys,
                    acceptRiskFlag: acceptRisk,
                    isTTY: process.stdin.isTTY === true,
                });
            }
            let anyError = false;
            for (const deKey of deKeys) {
                const candidates = await findImportCandidates(dataDir, deKey);
                const { paths: importPaths } = await resolveImportSet(candidates);
                if (importPaths.length === 0) {
                    console.error(
                        `No import file (csv/tsv/json) found for DE "${deKey}" under "${path.resolve(dataDir)}"`,
                    );
                    return 1;
                }
                const detectedFormat = formatFromExtension(importPaths[0]);
                if (!detectedFormat) {
                    console.error(
                        `Cannot determine format for "${importPaths[0]}". Use .csv, .tsv, or .json.`,
                    );
                    return 1;
                }

                const countBefore = await getDeRowCount(sdk, deKey);
                console.error(
                    `Row count before import: ${countBefore ?? '(unavailable)'} (DE "${deKey}")`,
                );

                if (clear) {
                    if (countBefore === 0) {
                        console.error(
                            `Skipping clear-data for DE "${deKey}" — DE is already empty.`,
                        );
                    } else {
                        await clearDataExtensionRows(sdk.soap, deKey);
                        console.warn(`Cleared data: DE "${deKey}"`);
                    }
                }

                const rows = await readRowsFromImportPaths(importPaths, detectedFormat);
                const { count: n, requestIds } = await importRowsForDe(sdk, {
                    deKey,
                    rows,
                    mode: /** @type {'upsert'|'insert'} */ (mode),
                });
                const srcLabel = importPaths.map((p) => `"${path.resolve(p)}"`).join(', ');
                console.error(`Imported: ${srcLabel} (${n} rows) -> DE ${deKey}`);

                const importHadError = await pollAsyncImportCompletion(sdk, requestIds);
                if (importHadError) {
                    anyError = true;
                }

                const countAfter = await getDeRowCount(sdk, deKey);
                console.error(
                    `Row count after import: ${countAfter ?? '(unavailable)'} (DE "${deKey}")`,
                );
                if (countAfter === null) {
                    console.error(`Could not verify import result for DE "${deKey}".`);
                } else if (countBefore !== null) {
                    // Insert: expect countBefore + n; upsert on empty: same; upsert on non-empty: expect >= n
                    const expected =
                        mode === 'insert' || countBefore === 0 ? (countBefore ?? 0) + n : n;
                    if (countAfter < expected) {
                        console.error(
                            `Import result for DE "${deKey}" looks unexpected: expected at least ${expected} rows, got ${countAfter}.`,
                        );
                    }
                }
            }
            return anyError ? 1 : 0;
        }

        const fileList = values.file ?? [];
        /** @type {string[]} */
        const keysFromFiles = [];
        const seenKeys = new Set();
        for (const fp of fileList) {
            const k = parseExportBasename(path.basename(fp)).customerKey;
            if (!seenKeys.has(k)) {
                seenKeys.add(k);
                keysFromFiles.push(k);
            }
        }
        if (backupBeforeImport === true) {
            for (const deKey of keysFromFiles) {
                const { paths: outPaths, rowCount } = await exportDataExtensionToFile(sdk, {
                    projectRoot,
                    credentialName: credential,
                    buName: bu,
                    deKey,
                    format: /** @type {'csv'|'tsv'|'json'} */ (fmt),
                    useGit: false,
                });
                const label = outPaths.map((p) => `"${path.resolve(p)}"`).join(', ');
                console.error(`Backup export: ${label} (${rowCount} rows)`);
            }
        }
        if (clear) {
            await confirmClearBeforeImport({
                deKeys: keysFromFiles,
                acceptRiskFlag: acceptRisk,
                isTTY: process.stdin.isTTY === true,
            });
        }
        /** @type {Map<string, string[]>} */
        const pathsByDeKey = new Map();
        for (const fp of fileList) {
            const { customerKey } = parseExportBasename(path.basename(fp));
            const list = pathsByDeKey.get(customerKey);
            if (list) {
                list.push(fp);
            } else {
                pathsByDeKey.set(customerKey, [fp]);
            }
        }

        let anyFileError = false;
        for (const customerKey of keysFromFiles) {
            const groupPaths = pathsByDeKey.get(customerKey) ?? [];
            const { paths: importPaths } = await resolveImportSet(groupPaths);
            if (importPaths.length === 0) {
                console.error(`No resolvable import files for DE "${customerKey}".`);
                anyFileError = true;
                continue;
            }
            const detectedFormat = formatFromExtension(importPaths[0]);
            if (!detectedFormat) {
                console.error(
                    `Cannot determine format for "${importPaths[0]}". Use .csv, .tsv, or .json.`,
                );
                anyFileError = true;
                continue;
            }

            const countBefore = await getDeRowCount(sdk, customerKey);
            console.error(
                `Row count before import: ${countBefore ?? '(unavailable)'} (DE "${customerKey}")`,
            );

            if (clear) {
                if (countBefore === 0) {
                    console.error(
                        `Skipping clear-data for DE "${customerKey}" — DE is already empty.`,
                    );
                } else {
                    await clearDataExtensionRows(sdk.soap, customerKey);
                    console.warn(`Cleared data: DE "${customerKey}"`);
                }
            }

            const rows = await readRowsFromImportPaths(importPaths, detectedFormat);
            const { count: n, requestIds } = await importRowsForDe(sdk, {
                deKey: customerKey,
                rows,
                mode: /** @type {'upsert'|'insert'} */ (mode),
            });
            const srcLabel = importPaths.map((p) => `"${path.resolve(p)}"`).join(', ');
            console.error(`Imported: ${srcLabel} (${n} rows)`);

            const importHadError = await pollAsyncImportCompletion(sdk, requestIds);
            if (importHadError) {
                anyFileError = true;
            }

            const countAfter = await getDeRowCount(sdk, customerKey);
            console.error(
                `Row count after import: ${countAfter ?? '(unavailable)'} (DE "${customerKey}")`,
            );
            if (countAfter === null) {
                console.error(`Could not verify import result for DE "${customerKey}".`);
            } else {
                const expected =
                    mode === 'insert' || countBefore === 0 ? (countBefore ?? 0) + n : n;
                if (countAfter < expected) {
                    console.error(
                        `Import result for DE "${customerKey}" looks unexpected: expected at least ${expected} rows, got ${countAfter}.`,
                    );
                }
            }
        }
        return anyFileError ? 1 : 0;
    }

    console.error(`Unknown command: ${sub}`);
    printHelp();
    return 1;
}
