import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { RestError } from 'sfmc-sdk/util';
import { withRetry429 } from '../lib/retry.mjs';

describe('withRetry429', () => {
    it('retries once on 429 then succeeds', async () => {
        let n = 0;
        const result = await withRetry429(
            async () => {
                n++;
                if (n === 1) {
                    throw new RestError({
                        response: { status: 429, headers: { 'retry-after': '0' }, data: {} },
                    });
                }
                return 'ok';
            },
            { maxAttempts: 3 }
        );
        assert.equal(result, 'ok');
        assert.equal(n, 2);
    });

    it('throws after max attempts', async () => {
        let n = 0;
        await assert.rejects(
            () =>
                withRetry429(async () => {
                    n++;
                    throw new RestError({
                        response: { status: 429, headers: {}, data: {} },
                    });
                }, { maxAttempts: 2 }),
            RestError
        );
        assert.equal(n, 2);
    });
});
