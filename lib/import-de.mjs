import { RestError } from 'sfmc-sdk/util';
import {
    chunkItemsForPayload,
    DEFAULT_MAX_BODY_BYTES,
    EMPTY_PAYLOAD_BYTES,
    MAX_OBJECTS_PER_BATCH,
    plainJsonRowBytes,
} from './batch.mjs';
import { formatFromExtension } from './file-resolve.mjs';
import { resolveImportRoute } from './import-routes.mjs';
import { withRetry429 } from './retry.mjs';
import { preflightImportPaths, streamRowsFromImportPaths } from './read-rows.mjs';
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
 * @param {object} parameters
 * @param {string} parameters.deKey
 * @param {object[]} parameters.rows
 * @param {'upsert'|'insert'} parameters.mode
 * @returns {Promise.<{ count: number, requestIds: (string|null)[] }>}
 */
export async function importRowsForDe(sdk, parameters) {
    const { deKey, rows, mode } = parameters;
    const chunks = chunkItemsForPayload(rows);
    const requestIds = [];
    for (const chunk of chunks) {
        requestIds.push(await postImportPayloadChunk(sdk, deKey, mode, chunk));
    }
    return { count: rows.length, requestIds };
}

/**
 * Streams rows in byte/row-limited windows with one lookahead or oversized row.
 * Serialized bytes are not a heap cap; individual JSON files are still parsed in full.
 *
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {object} parameters
 * @param {string} parameters.deKey
 * @param {AsyncIterable<object>} parameters.rowSource
 * @param {'upsert'|'insert'} parameters.mode
 * @param {number} [parameters.maxRowsPerBatch]
 * @param {number} [parameters.maxBytesPerBatch] - serialized items envelope budget
 * @param {number} [parameters.totalMemoryBatches] - legacy row-only estimate; no longer logged as exact
 * @param {string} [parameters.emptySourceLabel] - included in error when zero rows (e.g. file path)
 * @returns {Promise.<{ count: number, requestIds: (string|null)[] }>}
 */
export async function importRowsStreamingForDe(sdk, parameters) {
    const { deKey, rowSource, mode } = parameters;
    const maxRowsPerBatch = parameters.maxRowsPerBatch ?? MAX_OBJECTS_PER_BATCH;
    const maxBytesPerBatch = parameters.maxBytesPerBatch ?? DEFAULT_MAX_BODY_BYTES;
    const emptySourceLabel = parameters.emptySourceLabel;

    const requestIds = [];
    let totalCount = 0;
    let acceptedRows = 0;
    let memoryBatchIndex = 0;
    /**
     * @type {object[]}
     */
    let buffer = [];
    let bufferBytes = EMPTY_PAYLOAD_BYTES;

    /**
     * @param {object[]} rows
     * @returns {Promise.<void>}
     */
    async function flushMemoryBatch(rows) {
        if (rows.length === 0) {
            return;
        }
        memoryBatchIndex += 1;
        log.info(`Uploading batch ${memoryBatchIndex}`);
        const httpChunks = chunkItemsForPayload(rows);
        for (const chunk of httpChunks) {
            requestIds.push(await postImportPayloadChunk(sdk, deKey, mode, chunk));
            acceptedRows += chunk.length;
        }
    }

    try {
        for await (const row of rowSource) {
            const rowBytes = plainJsonRowBytes(row);
            const trialBytes =
                bufferBytes === null || rowBytes === null
                    ? Buffer.byteLength(JSON.stringify({ items: [...buffer, row] }), 'utf8')
                    : bufferBytes + (buffer.length > 0 ? 1 : 0) + rowBytes;
            if (buffer.length > 0 && trialBytes > maxBytesPerBatch) {
                await flushMemoryBatch(buffer);
                buffer = [];
                bufferBytes = EMPTY_PAYLOAD_BYTES;
            }
            bufferBytes =
                bufferBytes === null || rowBytes === null
                    ? null
                    : bufferBytes + (buffer.length > 0 ? 1 : 0) + rowBytes;
            buffer.push(row);
            totalCount += 1;
            const bytes =
                bufferBytes ?? Buffer.byteLength(JSON.stringify({ items: buffer }), 'utf8');
            if (!(buffer.length >= maxRowsPerBatch || bytes >= maxBytesPerBatch)) {
                continue;
            }

            await flushMemoryBatch(buffer);
            buffer = [];
            bufferBytes = EMPTY_PAYLOAD_BYTES;
        }
        await flushMemoryBatch(buffer);
    } catch (ex) {
        const error = new Error(
            `Import interrupted for DE "${deKey}": ${ex.message ?? String(ex)}. ` +
                `${acceptedRows} rows in ${requestIds.length} requests accepted before failure; ` +
                'async acceptance does not confirm completion. ' +
                'The failed operation may also have reached the server. No automatic retry or rollback.',
            { cause: ex },
        );
        error.importSubmission = {
            acceptedRows,
            acceptedRequests: requestIds.length,
            requestIds: [...requestIds],
        };
        throw error;
    }

    if (totalCount === 0) {
        assertNonEmptyImportRowCount(0, emptySourceLabel);
    }

    return { count: totalCount, requestIds };
}

/**
 * @param {{ rest: { put: Function, post: Function } }} sdk
 * @param {object} parameters
 * @param {string} parameters.filePath
 * @param {string} parameters.deKey - target DE customer key for API
 * @param {'csv'|'tsv'|'json'} [parameters.format] - optional; auto-detected from file extension if omitted
 * @param {'upsert'|'insert'} parameters.mode
 * @param {boolean} [parameters.skipEmptyLines] - CSV/TSV empty-record skipping (default true)
 * @returns {Promise.<{ count: number, requestIds: (string|null)[] }>}
 */
export async function importFromFile(sdk, parameters) {
    const format = parameters.format || formatFromExtension(parameters.filePath);
    if (!format) {
        throw new Error(
            `Cannot determine format for file: ${parameters.filePath}. Use .csv, .tsv, or .json extension.`,
        );
    }
    const prepared = await preflightImportPaths([parameters.filePath], format, {
        skipEmptyLines: parameters.skipEmptyLines,
    });
    const totalMemoryBatches = Math.max(1, Math.ceil(prepared.recordCount / MAX_OBJECTS_PER_BATCH));
    const rowSource = streamRowsFromImportPaths(prepared.paths, prepared.format, prepared);
    return importRowsStreamingForDe(sdk, {
        deKey: parameters.deKey,
        rowSource,
        mode: parameters.mode,
        totalMemoryBatches,
        emptySourceLabel: parameters.filePath,
    });
}
