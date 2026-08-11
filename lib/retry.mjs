import { RestError } from 'sfmc-sdk/util';

/**
 * @param {() => Promise.<any>} function_
 * @param {object} [options]
 * @param {number} [options.maxAttempts] default 5
 * @returns {Promise.<any>}
 */
export async function withRetry429(function_, options = {}) {
    const maxAttempts = options.maxAttempts ?? 5;
    let attempt = 0;
    let delayMs = 1000;
    while (true) {
        attempt++;
        try {
            return await function_();
        } catch (ex) {
            const status = ex instanceof RestError ? ex.response?.status : undefined;
            const retryAfter =
                ex instanceof RestError ? ex.response?.headers?.['retry-after'] : undefined;
            if (status === 429 && attempt < maxAttempts) {
                const wait =
                    retryAfter === undefined
                        ? delayMs
                        : Math.trunc(Number(retryAfter)) * 1000 || delayMs;
                await sleep(wait);
                delayMs = Math.min(delayMs * 2, 60_000);
                continue;
            }
            throw ex;
        }
    }
}

/**
 * @param {number} ms
 * @returns {Promise.<void>}
 */
function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}
