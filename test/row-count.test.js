import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { getDeRowCount } from '../lib/row-count.mjs';

/**
 * @param {object} opts
 * @param {object} [opts.response] - resolved response from sdk.rest.get
 * @param {Error} [opts.error] - if set, sdk.rest.get rejects with this
 * @returns {{ rest: { get: Function } }}
 */
function mockSdk(opts = {}) {
    return {
        rest: {
            get: async () => {
                if (opts.error) {
                    throw opts.error;
                }
                return opts.response ?? {};
            },
        },
    };
}

describe('getDeRowCount', () => {
    it('returns the count field from the rowset response', async () => {
        const sdk = mockSdk({ response: { count: 42, items: [] } });
        const result = await getDeRowCount(sdk, 'MY_DE');
        assert.equal(result, 42);
    });

    it('returns 0 when count is 0', async () => {
        const sdk = mockSdk({ response: { count: 0, items: [] } });
        const result = await getDeRowCount(sdk, 'EMPTY_DE');
        assert.equal(result, 0);
    });

    it('returns 0 when count field is absent', async () => {
        const sdk = mockSdk({ response: {} });
        const result = await getDeRowCount(sdk, 'MY_DE');
        assert.equal(result, 0);
    });

    it('returns null on API error and does not throw', async () => {
        const sdk = mockSdk({ error: new Error('Forbidden') });
        const result = await getDeRowCount(sdk, 'MY_DE');
        assert.equal(result, null);
    });

    it('encodes the DE key in the request path', async () => {
        let capturedPath;
        const sdk = {
            rest: {
                get: async (p) => {
                    capturedPath = p;
                    return { count: 1 };
                },
            },
        };
        await getDeRowCount(sdk, 'key with spaces');
        assert.ok(
            capturedPath.includes('key%20with%20spaces'),
            `Expected encoded key in path, got: ${capturedPath}`,
        );
        assert.ok(
            capturedPath.includes('$pagesize=1'),
            `Expected $pagesize=1 in path, got: ${capturedPath}`,
        );
    });
});
