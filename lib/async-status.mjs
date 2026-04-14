import { asyncRequestResultsPath, asyncRequestStatusPath } from './import-routes.mjs';

const POLL_INTERVAL_MS = 5000;
const PENDING_STATUSES = new Set(['Pending', 'Executing']);

/**
 * @param {number} ms
 * @returns {Promise.<void>}
 */
function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Polls the async import status API for each requestId until the job reaches
 * a terminal state ("Complete" or "Error").  On error, fetches per-row results
 * and logs each item message so the user can see what failed.
 *
 * @param {{ rest: { get: Function } }} sdk
 * @param {(string|null|undefined)[]} requestIds - one per imported chunk
 * @returns {Promise.<boolean>} true if at least one chunk job returned "Error"
 */
export async function pollAsyncImportCompletion(sdk, requestIds) {
    let hasError = false;

    for (const requestId of requestIds) {
        if (!requestId) {
            console.error('Async import: no requestId returned for chunk — skipping status check.');
            continue;
        }

        console.error(`Waiting for async import to complete (requestId: ${requestId})…`);
        await sleep(POLL_INTERVAL_MS);

        let requestStatus;
        do {
            let statusResult;
            try {
                statusResult = await sdk.rest.get(asyncRequestStatusPath(requestId));
            } catch (ex) {
                console.error(
                    `Async import: status check failed for requestId ${requestId}: ${ex.message}`,
                );
                break;
            }

            requestStatus = statusResult?.status?.requestStatus;

            if (PENDING_STATUSES.has(requestStatus)) {
                console.error(
                    `Async import still in progress (requestId: ${requestId}) — retrying in 5 s…`,
                );
                await sleep(POLL_INTERVAL_MS);
            }
        } while (PENDING_STATUSES.has(requestStatus));

        if (requestStatus === 'Complete') {
            console.error(`Async import completed successfully (requestId: ${requestId}).`);
        } else if (requestStatus === 'Error') {
            console.error(`Async import job failed (requestId: ${requestId}).`);
            hasError = true;

            let resultsResult;
            try {
                resultsResult = await sdk.rest.get(asyncRequestResultsPath(requestId));
            } catch (ex) {
                console.error(
                    `Async import: could not retrieve error details for requestId ${requestId}: ${ex.message}`,
                );
                continue;
            }

            const items = resultsResult?.items ?? [];
            if (items.length === 0) {
                console.error('Async import: no item-level error details returned by API.');
            } else {
                for (const item of items) {
                    console.error(`Import error: ${item.message}`);
                }
            }
        } else if (requestStatus !== undefined) {
            console.error(
                `Async import: unexpected status "${requestStatus}" for requestId ${requestId}.`,
            );
        }
    }

    return hasError;
}
