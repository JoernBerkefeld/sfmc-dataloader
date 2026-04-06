import { RestError } from 'sfmc-sdk/util';

/**
 * @param {() => Promise<any>} fn
 * @param {object} [opts]
 * @param {number} [opts.maxAttempts] default 5
 * @returns {Promise<any>}
 */
export async function withRetry429(fn, opts = {}) {
    const maxAttempts = opts.maxAttempts ?? 5;
    let attempt = 0;
    let delayMs = 1000;
    while (true) {
        attempt++;
        try {
            return await fn();
        } catch (e) {
            const status = e instanceof RestError ? e.response?.status : undefined;
            const retryAfter = e instanceof RestError ? e.response?.headers?.['retry-after'] : undefined;
            if (status === 429 && attempt < maxAttempts) {
                const wait =
                    retryAfter !== undefined
                        ? Number.parseInt(String(retryAfter), 10) * 1000 || delayMs
                        : delayMs;
                await sleep(wait);
                delayMs = Math.min(delayMs * 2, 60_000);
                continue;
            }
            throw e;
        }
    }
}

/**
 * @param {number} ms
 * @returns {Promise<void>}
 */
function sleep(ms) {
    return new Promise((r) => setTimeout(r, ms));
}
