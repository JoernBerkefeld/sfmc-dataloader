import { parseArgs } from 'node:util';
import process from 'node:process';
import path from 'node:path';
import SDK from 'sfmc-sdk';
import {
    loadMcdevProject,
    parseCredBu,
    resolveCredentialAndMid,
    buildSdkAuthObject,
} from './config.mjs';
import { dataDirectoryForBu } from './paths.mjs';
import { exportDataExtensionToFile } from './export-de.mjs';
import { findImportCandidates, pickLatestByMtime } from './file-resolve.mjs';
import { parseExportBasename } from './filename.mjs';
import { importFromFile } from './import-de.mjs';
import { clearDataExtensionRows } from './clear-de.mjs';
import { confirmClearBeforeImport } from './confirm-clear.mjs';

function printHelp() {
    console.log(`mcdata — SFMC Data Extension export/import (mcdev project)

Usage:
  mcdata export <credential>/<bu> --de <key> [--de <key> ...] [options]
  mcdata import <credential>/<bu> (--de <key> ... | --file <path> ...) [options]

Options:
  -p, --project <dir>     mcdev project root (default: cwd)
  --format <csv|tsv|json> File format (default: csv)
  --json-pretty           Pretty-print JSON on export

Import options:
  --api <async|sync>      REST row API family (default: async)
  --mode <upsert|insert|update>  (default: upsert; insert/update require --api sync)
  --clear-before-import   SOAP ClearData before import (destructive; see below)
  --i-accept-clear-data-risk  Non-interactive acknowledgement for --clear-before-import

Notes:
  Exports are written under ./data/<credential>/<bu>/ with "+MCDATA+" in the filename.
  Import with --de resolves the latest matching file in that folder (by mtime).
  Import with --file parses the DE key from the basename (+MCDATA+ prefix).

Clear data warning:
  --clear-before-import deletes ALL existing rows in the target DE(s) before upload.
  Interactive: type YES when prompted. CI: also pass --i-accept-clear-data-risk.
`);
}

/**
 * @param {string[]} argv
 * @returns {Promise<number>} exit code
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
                api: { type: 'string' },
                mode: { type: 'string' },
                'clear-before-import': { type: 'boolean', default: false },
                'i-accept-clear-data-risk': { type: 'boolean', default: false },
                'json-pretty': { type: 'boolean', default: false },
                help: { type: 'boolean', short: 'h', default: false },
            },
        });
        values = parsed.values;
        positionals = parsed.positionals;
    } catch (e) {
        console.error(e.message);
        printHelp();
        return 1;
    }

    if (values.help) {
        printHelp();
        return 0;
    }
    if (positionals.length === 0) {
        printHelp();
        return 1;
    }

    const sub = positionals[0];
    const credBuRaw = positionals[1];
    if (!credBuRaw) {
        console.error('Missing <credential>/<businessUnit>.');
        printHelp();
        return 1;
    }

    const projectRoot = path.resolve(values.project ?? process.cwd());
    const fmt = values.format ?? 'csv';
    if (!['csv', 'tsv', 'json'].includes(fmt)) {
        console.error(`Invalid --format: ${fmt}`);
        return 1;
    }

    const { mcdevrc, mcdevAuth } = loadMcdevProject(projectRoot);
    const { credential, bu } = parseCredBu(credBuRaw);
    const { mid, authCred } = resolveCredentialAndMid(mcdevrc, mcdevAuth, credential, bu);
    const authObj = buildSdkAuthObject(authCred, mid);
    const sdk = new SDK(authObj, { requestAttempts: 3 });

    if (sub === 'export') {
        const des = [].concat(values.de ?? []);
        if (des.length === 0) {
            console.error('export requires at least one --de <customerKey>');
            return 1;
        }
        for (const deKey of des) {
            const out = await exportDataExtensionToFile(sdk, {
                projectRoot,
                credentialName: credential,
                buName: bu,
                deKey,
                format: /** @type {'csv'|'tsv'|'json'} */ (fmt),
                jsonPretty: values['json-pretty'],
            });
            console.error(`Exported: ${out}`);
        }
        return 0;
    }

    if (sub === 'import') {
        const api = values.api ?? 'async';
        const mode = values.mode ?? 'upsert';
        if (!['async', 'sync'].includes(api)) {
            console.error(`Invalid --api: ${api}`);
            return 1;
        }
        if (!['upsert', 'insert', 'update'].includes(mode)) {
            console.error(`Invalid --mode: ${mode}`);
            return 1;
        }

        const hasDe = values.de?.length > 0;
        const hasFile = values.file?.length > 0;
        if (hasDe === hasFile) {
            console.error('import requires exactly one of: repeated --de <key> OR repeated --file <path>');
            return 1;
        }

        const clear = values['clear-before-import'];
        const acceptRisk = values['i-accept-clear-data-risk'];

        if (hasDe) {
            const deKeys = [].concat(values.de ?? []);
            const dataDir = dataDirectoryForBu(projectRoot, credential, bu);
            if (clear) {
                await confirmClearBeforeImport({
                    deKeys,
                    acceptRiskFlag: acceptRisk,
                    isTTY: process.stdin.isTTY === true,
                });
                for (const deKey of deKeys) {
                    await clearDataExtensionRows(sdk.soap, deKey);
                }
            }
            for (const deKey of deKeys) {
                const candidates = await findImportCandidates(dataDir, deKey, fmt);
                if (candidates.length === 0) {
                    console.error(`No ${fmt} file found for DE "${deKey}" under ${dataDir}`);
                    return 1;
                }
                const filePath =
                    candidates.length === 1 ? candidates[0] : await pickLatestByMtime(candidates);
                await importFromFile(sdk, {
                    filePath,
                    deKey,
                    format: /** @type {'csv'|'tsv'|'json'} */ (fmt),
                    api: /** @type {'async'|'sync'} */ (api),
                    mode: /** @type {'upsert'|'insert'|'update'} */ (mode),
                });
                console.error(`Imported ${filePath} -> DE ${deKey}`);
            }
            return 0;
        }

        const fileList = values.file ?? [];
        const keysFromFiles = fileList.map((fp) => parseExportBasename(path.basename(fp)).customerKey);
        if (clear) {
            await confirmClearBeforeImport({
                deKeys: keysFromFiles,
                acceptRiskFlag: acceptRisk,
                isTTY: process.stdin.isTTY === true,
            });
            for (const deKey of keysFromFiles) {
                await clearDataExtensionRows(sdk.soap, deKey);
            }
        }
        for (const filePath of fileList) {
            const base = path.basename(filePath);
            const { customerKey } = parseExportBasename(base);
            await importFromFile(sdk, {
                filePath,
                deKey: customerKey,
                format: /** @type {'csv'|'tsv'|'json'} */ (fmt),
                api: /** @type {'async'|'sync'} */ (api),
                mode: /** @type {'upsert'|'insert'|'update'} */ (mode),
            });
            console.error(`Imported ${filePath}`);
        }
        return 0;
    }

    console.error(`Unknown command: ${sub}`);
    printHelp();
    return 1;
}
