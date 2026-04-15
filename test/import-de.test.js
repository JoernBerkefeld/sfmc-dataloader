import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { RestError } from 'sfmc-sdk/util';
import { importRowsForDe, importFromFile } from '../lib/import-de.mjs';

describe('importRowsForDe', () => {
    it('calls put for upsert in chunks', async () => {
        const calls = [];
        const sdk = {
            rest: {
                put: async (p, body) => {
                    calls.push({ path: p, n: body.items.length });
                    return {};
                },
                post: async () => {
                    throw new Error('unexpected post');
                },
            },
        };
        const rows = Array.from({ length: 3 }, (_, i) => ({ id: String(i) }));
        const result = await importRowsForDe(sdk, {
            deKey: 'K',
            rows,
            mode: 'upsert',
        });
        assert.equal(result.count, 3);
        assert.deepEqual(result.requestIds, [null]);
        assert.equal(calls.length, 1);
        assert.equal(calls[0].n, 3);
        assert.ok(calls[0].path.includes('async'));
    });

    it('throws readable message when PUT returns 400 with resultMessages', async () => {
        const sdk = {
            rest: {
                put: async () => {
                    throw new RestError({
                        response: {
                            status: 400,
                            headers: {},
                            data: {
                                resultMessages: [
                                    { message: 'Primary key field required for upsert operations' },
                                ],
                            },
                        },
                    });
                },
                post: async () => {
                    throw new Error('unexpected');
                },
            },
        };
        await assert.rejects(
            () => importRowsForDe(sdk, { deKey: 'MyDE', rows: [{ a: 1 }], mode: 'upsert' }),
            /Primary key field required for upsert/,
        );
    });

    it('calls post for insert', async () => {
        const sdk = {
            rest: {
                put: async () => {
                    throw new Error('unexpected put');
                },
                post: async () => ({ requestId: 'req-123' }),
            },
        };
        const result = await importRowsForDe(sdk, {
            deKey: 'K',
            rows: [{ a: 1 }],
            mode: 'insert',
        });
        assert.equal(result.count, 1);
        assert.deepEqual(result.requestIds, ['req-123']);
    });
});

describe('importFromFile', () => {
    let tmp;
    before(async () => {
        tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-import-'));
    });
    after(async () => {
        await fs.rm(tmp, { recursive: true, force: true });
    });

    const stubSdk = {
        rest: {
            put: async () => {
                throw new Error('should not call API when file has no rows');
            },
            post: async () => {
                throw new Error('should not call API when file has no rows');
            },
        },
    };

    it('rejects empty CSV', async () => {
        const p = path.join(tmp, 'empty.csv');
        await fs.writeFile(p, '', 'utf8');
        await assert.rejects(
            () =>
                importFromFile(stubSdk, {
                    filePath: p,
                    deKey: 'K',
                    mode: 'upsert',
                }),
            /no data rows/,
        );
    });

    it('rejects BOM-only CSV', async () => {
        const p = path.join(tmp, 'bom.csv');
        await fs.writeFile(p, '\uFEFF', 'utf8');
        await assert.rejects(
            () =>
                importFromFile(stubSdk, {
                    filePath: p,
                    deKey: 'K',
                    mode: 'upsert',
                }),
            /no data rows/,
        );
    });

    it('rejects header-only CSV', async () => {
        const p = path.join(tmp, 'header.csv');
        await fs.writeFile(p, 'a,b,c\n', 'utf8');
        await assert.rejects(
            () =>
                importFromFile(stubSdk, {
                    filePath: p,
                    deKey: 'K',
                    mode: 'upsert',
                }),
            /no data rows/,
        );
    });

    it('rejects header-only TSV', async () => {
        const p = path.join(tmp, 'header.tsv');
        await fs.writeFile(p, 'a\tb\tc\n', 'utf8');
        await assert.rejects(
            () =>
                importFromFile(stubSdk, {
                    filePath: p,
                    deKey: 'K',
                    mode: 'upsert',
                }),
            /no data rows/,
        );
    });

    it('rejects empty JSON array', async () => {
        const p = path.join(tmp, 'empty.json');
        await fs.writeFile(p, '[]', 'utf8');
        await assert.rejects(
            () =>
                importFromFile(stubSdk, {
                    filePath: p,
                    deKey: 'K',
                    mode: 'upsert',
                }),
            /no data rows/,
        );
    });
});
