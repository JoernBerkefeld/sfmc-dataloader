import { beforeEach, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { setImmediate } from 'node:timers/promises';
import { pollAsyncImportCompletion } from '../lib/async-status.mjs';
import { log } from '../lib/log.mjs';

/**
 * Builds a mock SDK that records paths and virtual request times.
 *
 * @param {(object|Error)[]} responses - successive responses or failures
 * @returns {{ rest: { get: Function }, calls: { path: string, time: number }[] }}
 */
function buildSdk(responses) {
    const calls = [];
    let index = 0;
    return {
        rest: {
            get(path) {
                calls.push({ path, time: Date.now() });
                const response = responses[index++];
                return response instanceof Error
                    ? Promise.reject(response)
                    : Promise.resolve(response);
            },
        },
        calls,
    };
}

/**
 * Advances mocked time and lets the polling promise process its response.
 *
 * @param {import('node:test').TestContext} t - test context
 * @param {number} milliseconds - virtual time to advance
 * @returns {Promise.<void>}
 */
async function advance(t, milliseconds) {
    t.mock.timers.tick(milliseconds);
    await setImmediate();
}

/**
 * Returns logged messages for a mocked log method.
 *
 * @param {Function} method - mocked log method
 * @returns {string[]}
 */
function messages(method) {
    return method.mock.calls.map((call) => call.arguments[0]);
}

const complete = { status: { requestStatus: 'Complete' } };
const failed = { status: { requestStatus: 'Error' } };

describe('pollAsyncImportCompletion', () => {
    beforeEach((t) => {
        t.mock.timers.enable({ apis: ['setTimeout', 'Date'], now: 0 });
        t.mock.method(log, 'info', () => {});
        t.mock.method(log, 'warn', () => {});
        t.mock.method(log, 'error', () => {});
    });

    it('skips all invalid IDs without waiting and preserves missing-ID logging', async () => {
        const sdk = buildSdk([]);
        assert.equal(await pollAsyncImportCompletion(sdk, [null, undefined, '']), false);
        assert.deepEqual(sdk.calls, []);
        assert.equal(Date.now(), 0);
        assert.deepEqual(
            messages(log.error),
            Array.from(
                { length: 3 },
                () => 'Async import: no requestId returned for chunk — skipping status check.',
            ),
        );
    });

    it('waits exactly once before N completed IDs, not before invalid IDs', async (t) => {
        const sdk = buildSdk([complete, complete, complete]);
        const result = pollAsyncImportCompletion(sdk, [null, 'req-a', undefined, 'req-b', 'req-c']);
        assert.deepEqual(sdk.calls, []);
        await advance(t, 4999);
        assert.deepEqual(sdk.calls, []);
        await advance(t, 1);
        assert.equal(await result, false);
        assert.deepEqual(
            sdk.calls.map(({ time }) => time),
            [5000, 5000, 5000],
        );
        assert.deepEqual(
            sdk.calls.map(({ path }) => path),
            ['req-a', 'req-b', 'req-c'].map((id) => `/data/v1/async/${id}/status`),
        );
        assert.equal(messages(log.info).filter((line) => line.includes('successfully')).length, 3);
    });

    it('retains 5s Pending and Executing waits without adding another grace period', async (t) => {
        const sdk = buildSdk([
            { status: { requestStatus: 'Pending' } },
            { status: { requestStatus: 'Executing' } },
            complete,
            { status: { requestStatus: 'Pending' } },
            complete,
        ]);
        const result = pollAsyncImportCompletion(sdk, ['req-a', 'req-b']);
        await advance(t, 5000);
        assert.equal(sdk.calls.length, 1);
        await advance(t, 4999);
        assert.equal(sdk.calls.length, 1);
        await advance(t, 1);
        assert.equal(sdk.calls.length, 2);
        await advance(t, 5000);
        assert.equal(sdk.calls.length, 4);
        await advance(t, 5000);
        assert.equal(await result, false);
        assert.deepEqual(
            sdk.calls.map(({ time }) => time),
            [5000, 10000, 15000, 15000, 20000],
        );
        assert.equal(messages(log.info).filter((line) => line.includes('retrying')).length, 3);
    });

    it('keeps status requests sequential until the current response resolves', async (t) => {
        const sdk = buildSdk([complete]);
        const first = Promise.withResolvers();
        const get = sdk.rest.get;
        sdk.rest.get = t.mock.fn((path) => (path.includes('req-a') ? first.promise : get(path)));
        const result = pollAsyncImportCompletion(sdk, ['req-a', 'req-b']);
        await advance(t, 5000);
        assert.equal(sdk.rest.get.mock.callCount(), 1);
        await advance(t, 5000);
        assert.equal(sdk.rest.get.mock.callCount(), 1);
        first.resolve(complete);
        await setImmediate();
        assert.equal(await result, false);
        assert.equal(sdk.rest.get.mock.callCount(), 2);
    });

    it('fetches and logs every error detail and continues to later IDs without waiting', async (t) => {
        const sdk = buildSdk([
            failed,
            { items: [{ message: 'Column not found' }, { message: 'NULL not allowed' }] },
            complete,
        ]);
        const result = pollAsyncImportCompletion(sdk, ['req-error', 'req-ok']);
        await advance(t, 5000);
        assert.equal(await result, true);
        assert.deepEqual(
            sdk.calls.map(({ time }) => time),
            [5000, 5000, 5000],
        );
        assert.ok(sdk.calls[1].path.endsWith('/results'));
        assert.deepEqual(messages(log.error), [
            'Async import job failed (requestId: req-error).',
            'Import error: Column not found',
            'Import error: NULL not allowed',
        ]);
    });

    it('logs failed error-detail retrieval and continues with the error result retained', async (t) => {
        const sdk = buildSdk([failed, new Error('Details unavailable'), complete]);
        const result = pollAsyncImportCompletion(sdk, ['req-error', 'req-ok']);
        await advance(t, 5000);
        assert.equal(await result, true);
        assert.equal(sdk.calls.length, 3);
        assert.ok(messages(log.error).some((line) => line.includes('Details unavailable')));
    });

    for (const response of [{ items: [] }, {}]) {
        it(`warns when error details are ${response.items ? 'empty' : 'missing'}`, async (t) => {
            const sdk = buildSdk([failed, response]);
            const result = pollAsyncImportCompletion(sdk, ['req-error']);
            await advance(t, 5000);
            assert.equal(await result, true);
            assert.deepEqual(messages(log.warn), [
                'Async import: no item-level error details returned by API.',
            ]);
        });
    }

    it('logs a status network failure and continues without another initial wait', async (t) => {
        const sdk = buildSdk([new Error('Network timeout'), complete]);
        const result = pollAsyncImportCompletion(sdk, ['req-error', 'req-ok']);
        await advance(t, 5000);
        assert.equal(await result, false);
        assert.deepEqual(
            sdk.calls.map(({ time }) => time),
            [5000, 5000],
        );
        assert.deepEqual(messages(log.error), [
            'Async import: status check failed for requestId req-error: Network timeout',
        ]);
    });

    it('preserves unexpected terminal status logging without fetching results', async (t) => {
        const sdk = buildSdk([{ status: { requestStatus: 'Cancelled' } }]);
        const result = pollAsyncImportCompletion(sdk, ['req-cancelled']);
        await advance(t, 5000);
        assert.equal(await result, false);
        assert.equal(sdk.calls.length, 1);
        assert.deepEqual(messages(log.error), [
            'Async import: unexpected status "Cancelled" for requestId req-cancelled.',
        ]);
    });

    it('preserves an absent status without logging an unexpected status', async (t) => {
        const sdk = buildSdk([{}]);
        const result = pollAsyncImportCompletion(sdk, ['req-empty']);
        await advance(t, 5000);
        assert.equal(await result, false);
        assert.equal(sdk.calls.length, 1);
        assert.deepEqual(messages(log.error), []);
    });
});
