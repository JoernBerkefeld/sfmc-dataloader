import { RestError } from 'sfmc-sdk/util';
import { chunkItemsForPayload, MAX_OBJECTS_PER_BATCH } from './batch.mjs';
import { formatFromExtension } from './file-resolve.mjs';
import { resolveImportRoute } from './import-routes.mjs';
import { withRetry429 } from './retry.mjs';
import { countDataRowsFromImportPaths, streamRowsFromFile } from './read-rows.mjs';
import { log } from './log.mjs';

/**
 * @param {number} rowCount
 * @param {string} [emptySourceLabel]
 */
export function assertNonEmptyImportRowCount(rowCount, emptySourceLabel) {
    if (rowCount > 0) {
        return;
    }
    const prefix = emptySourceLabel
        ? `Import file contains no data rows: "${emptySourceLabel}". `
        : 'Import file contains no data rows. ';
    throw new Error(
        prefix +
            'The file may be empty, contain only a BOM, or contain only a header row. ' +
            'Export the DE first to obtain a template with column names, then add rows.',
    );
}

/**
 * Logs a warning when the post-import row count cannot be verified or looks unexpectedly low.
 * Skips the "unexpected" check when `countBefore` is unavailable (cannot compute expected).
 *
 * @param {object} opts
 * @param {number|null} opts.countBefore - row count before any clear/import
 * @param {boolean} opts.cleared - whether `clearDataExtensionRows` ran (DE emptied before upload)
 * @param {number|null} opts.countAfter - row count after the import
 * @param {number} opts.imported - number of rows sent to the API
 * @param {'upsert'|'insert'} opts.mode
 * @param {string} opts.label - text after "for" in messages, e.g. `DE "MyKey"` or `cred/bu DE "MyKey"`
 */
export function warnIfImportCountUnexpected({
    countBefore,
    cleared,
    countAfter,
    imported,
    mode,
    label,
}) {
    if (countAfter === null) {
        log.warn(`Could not verify import result for ${label}.`);
        return;
    }
    if (countBefore === null) {
        return;
    }
    const effectiveCountBefore = cleared ? 0 : countBefore;
    const expected =
        mode === 'insert' || effectiveCountBefore === 0
            ? effectiveCountBefore + imported
            : imported;
    if (countAfter < expected) {
        log.warn(
            `Import result for ${label} looks unexpected: expected at least ${expected} rows, got ${countAfter}.`,
        );
    }
}

/**
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {string} deKey
 * @param {'upsert'|'insert'} mode
 * @param {object[]} chunk
 * @returns {Promise.<string|null>}
 */
async function postImportPayloadChunk(sdk, deKey, mode, chunk) {
    const route = resolveImportRoute(mode);
    const p = route.path(deKey);
    const body = { items: chunk };
    try {
        const resp = await withRetry429(() =>
            route.method === 'PUT' ? sdk.rest.put(p, body) : sdk.rest.post(p, body),
        );
        return resp?.requestId ?? null;
    } catch (ex) {
        const msgs =
            ex instanceof RestError &&
            ex.response?.status === 400 &&
            Array.isArray(ex.response?.data?.resultMessages) &&
            ex.response.data.resultMessages.length > 0
                ? ex.response.data.resultMessages
                : null;
        if (msgs) {
            const summary = msgs.map((m) => m.message ?? String(m)).join('; ');
            throw new Error(`Import failed for DE "${deKey}" (HTTP 400): ${summary}`, {
                cause: ex,
            });
        }
        throw ex;
    }
}

/**
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {object} params
 * @param {string} params.deKey
 * @param {object[]} params.rows
 * @param {'upsert'|'insert'} params.mode
 * @returns {Promise.<{ count: number, requestIds: (string|null)[] }>}
 */
export async function importRowsForDe(sdk, params) {
    const { deKey, rows, mode } = params;
    const chunks = chunkItemsForPayload(rows);
    const requestIds = [];
    for (const chunk of chunks) {
        requestIds.push(await postImportPayloadChunk(sdk, deKey, mode, chunk));
    }
    return { count: rows.length, requestIds };
}

/**
 * Streams row objects from disk in memory windows, then chunks for HTTP payload limits.
 *
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {object} params
 * @param {string} params.deKey
 * @param {AsyncIterable<object>} params.rowSource
 * @param {'upsert'|'insert'} params.mode
 * @param {number} [params.maxRowsPerBatch]
 * @param {number} params.totalMemoryBatches - upload windows (ceil(rowCount / maxRowsPerBatch))
 * @param {string} [params.emptySourceLabel] - included in error when zero rows (e.g. file path)
 * @returns {Promise.<{ count: number, requestIds: (string|null)[] }>}
 */
export async function importRowsStreamingForDe(sdk, params) {
    const { deKey, rowSource, mode, totalMemoryBatches } = params;
    const maxRowsPerBatch = params.maxRowsPerBatch ?? MAX_OBJECTS_PER_BATCH;
    const emptySourceLabel = params.emptySourceLabel;

    const requestIds = [];
    let totalCount = 0;
    let memoryBatchIndex = 0;
    /** @type {object[]} */
    let buffer = [];

    /**
     * @param {object[]} rows
     * @returns {Promise.<void>}
     */
    async function flushMemoryBatch(rows) {
        if (rows.length === 0) {
            return;
        }
        memoryBatchIndex += 1;
        log.info(`Uploading batch ${memoryBatchIndex} of ${totalMemoryBatches}`);
        const httpChunks = chunkItemsForPayload(rows);
        for (const chunk of httpChunks) {
            requestIds.push(await postImportPayloadChunk(sdk, deKey, mode, chunk));
        }
    }

    for await (const row of rowSource) {
        buffer.push(row);
        totalCount += 1;
        if (buffer.length >= maxRowsPerBatch) {
            await flushMemoryBatch(buffer);
            buffer = [];
        }
    }
    await flushMemoryBatch(buffer);

    if (totalCount === 0) {
        assertNonEmptyImportRowCount(0, emptySourceLabel);
    }

    return { count: totalCount, requestIds };
}

/**
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {object} params
 * @param {string} params.filePath
 * @param {string} params.deKey - target DE customer key for API
 * @param {'csv'|'tsv'|'json'} [params.format] - optional; auto-detected from file extension if omitted
 * @param {'upsert'|'insert'} params.mode
 * @returns {Promise.<{ count: number, requestIds: (string|null)[] }>}
 */
export async function importFromFile(sdk, params) {
    const format = params.format || formatFromExtension(params.filePath);
    if (!format) {
        throw new Error(
            `Cannot determine format for file: ${params.filePath}. Use .csv, .tsv, or .json extension.`,
        );
    }
    const rowCount = await countDataRowsFromImportPaths([params.filePath], format);
    assertNonEmptyImportRowCount(rowCount, params.filePath);
    const totalMemoryBatches = Math.max(1, Math.ceil(rowCount / MAX_OBJECTS_PER_BATCH));
    const rowSource = streamRowsFromFile(params.filePath, format);
    return importRowsStreamingForDe(sdk, {
        deKey: params.deKey,
        rowSource,
        mode: params.mode,
        totalMemoryBatches,
        emptySourceLabel: params.filePath,
    });
}
