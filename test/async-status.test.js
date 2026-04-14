import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { pollAsyncImportCompletion } from '../lib/async-status.mjs';

/**
 * Builds a minimal mock sdk.rest.get that returns responses in sequence.
 *
 * @param {object[]} responses - array of objects to return on successive calls
 * @returns {{ rest: { get: Function }, calls: string[] }}
 */
function buildSdk(responses) {
    const calls = [];
    let index = 0;
    return {
        rest: {
            get(path) {
                calls.push(path);
                const resp = responses[index];
                index = Math.min(index + 1, responses.length - 1);
                return Promise.resolve(resp);
            },
        },
        calls,
    };
}

describe('pollAsyncImportCompletion', () => {
    it('returns false and skips when requestId is null', async () => {
        const sdk = buildSdk([]);
        const result = await pollAsyncImportCompletion(sdk, [null]);
        assert.equal(result, false);
        assert.equal(sdk.calls.length, 0);
    });

    it('returns false and skips when requestId is undefined', async () => {
        const sdk = buildSdk([]);
        const result = await pollAsyncImportCompletion(sdk, [undefined]);
        assert.equal(result, false);
        assert.equal(sdk.calls.length, 0);
    });

    it('returns false when status is Complete on first check', async () => {
        const sdk = buildSdk([{ status: { requestStatus: 'Complete' } }]);
        const result = await pollAsyncImportCompletion(sdk, ['req-001']);
        assert.equal(result, false);
        assert.equal(sdk.calls.length, 1);
        assert.ok(sdk.calls[0].includes('req-001'));
        assert.ok(sdk.calls[0].includes('/status'));
    });

    it('retries while Pending then resolves on Complete', async () => {
        const sdk = buildSdk([
            { status: { requestStatus: 'Pending' } },
            { status: { requestStatus: 'Executing' } },
            { status: { requestStatus: 'Complete' } },
        ]);
        const result = await pollAsyncImportCompletion(sdk, ['req-002']);
        assert.equal(result, false);
        assert.equal(sdk.calls.length, 3);
    });

    it('returns true and fetches results when status is Error', async () => {
        const sdk = buildSdk([
            { status: { requestStatus: 'Error' } },
            {
                items: [
                    { message: 'Column not found', status: 'Error' },
                    { message: 'NULL not allowed', status: 'Error' },
                ],
            },
        ]);
        const result = await pollAsyncImportCompletion(sdk, ['req-003']);
        assert.equal(result, true);
        assert.equal(sdk.calls.length, 2);
        assert.ok(sdk.calls[0].includes('/status'));
        assert.ok(sdk.calls[1].includes('/results'));
    });

    it('handles multiple requestIds and returns true if any errored', async () => {
        const responses = [
            { status: { requestStatus: 'Complete' } },
            { status: { requestStatus: 'Error' } },
            { items: [{ message: 'Bad value', status: 'Error' }] },
        ];
        let index = 0;
        const sdk = {
            rest: {
                get() {
                    return Promise.resolve(responses[index++]);
                },
            },
        };
        const result = await pollAsyncImportCompletion(sdk, ['req-a', 'req-b']);
        assert.equal(result, true);
    });

    it('returns false when all requestIds complete successfully', async () => {
        let index = 0;
        const multiSdk = {
            rest: {
                get() {
                    const resp = [
                        { status: { requestStatus: 'Complete' } },
                        { status: { requestStatus: 'Complete' } },
                    ][index++];
                    return Promise.resolve(resp);
                },
            },
        };
        const result = await pollAsyncImportCompletion(multiSdk, ['req-x', 'req-y']);
        assert.equal(result, false);
    });

    it('handles status check network error gracefully without throwing', async () => {
        const sdk = {
            rest: {
                get() {
                    return Promise.reject(new Error('Network timeout'));
                },
            },
        };
        const result = await pollAsyncImportCompletion(sdk, ['req-err']);
        assert.equal(result, false);
    });
});
