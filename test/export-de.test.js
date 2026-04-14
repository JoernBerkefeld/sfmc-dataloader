import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
    fetchAllRowObjects,
    fetchDataExtensionFieldNames,
    serializeRows,
    exportDataExtensionToFile,
} from '../lib/export-de.mjs';

describe('fetchAllRowObjects', () => {
    it('uses getBulk with page size 2500 and maps items to flat rows', async () => {
        const calls = [];
        const sdk = {
            rest: {
                getBulk: async (path, pageSize) => {
                    calls.push({ path, pageSize });
                    assert.ok(path.includes('rowset'));
                    assert.equal(pageSize, 2500);
                    return {
                        items: [
                            {
                                keys: { pk: '1' },
                                values: { a: 'x' },
                            },
                            {
                                keys: { pk: '2' },
                                values: { a: 'y' },
                            },
                        ],
                        count: 2,
                    };
                },
            },
        };
        const rows = await fetchAllRowObjects(sdk, 'DE1');
        assert.deepEqual(rows, [
            { pk: '1', a: 'x' },
            { pk: '2', a: 'y' },
        ]);
        assert.equal(calls.length, 1);
    });
});

describe('serializeRows', () => {
    it('writes CSV with BOM', () => {
        const s = serializeRows([{ a: '1' }], 'csv', false);
        assert.ok(s.startsWith('\uFEFF'));
    });

    it('writes CSV with quoted fields', () => {
        const s = serializeRows([{ col1: 'val1', col2: 'val2' }], 'csv', false);
        assert.ok(s.includes('"col1"'), 'CSV headers should be quoted');
        assert.ok(s.includes('"val1"'), 'CSV values should be quoted');
    });

    it('writes TSV without quoted fields', () => {
        const s = serializeRows([{ col1: 'val1', col2: 'val2' }], 'tsv', false);
        assert.ok(!s.includes('"col1"'), 'TSV headers should not be quoted');
        assert.ok(!s.includes('"val1"'), 'TSV values should not be quoted');
        assert.ok(s.includes('col1\tcol2'), 'TSV should have tab-separated headers');
        assert.ok(s.includes('val1\tval2'), 'TSV should have tab-separated values');
    });

    it('writes TSV with BOM', () => {
        const s = serializeRows([{ a: '1' }], 'tsv', false);
        assert.ok(s.startsWith('\uFEFF'), 'TSV should start with BOM');
    });

    it('writes CSV header only when rows empty and columns provided', () => {
        const s = serializeRows([], 'csv', false, ['col1', 'col2']);
        assert.ok(s.startsWith('\uFEFF'));
        const lines = s.trimEnd().split(/\r?\n/);
        assert.equal(lines.length, 1);
        assert.ok(lines[0].includes('"col1"'));
        assert.ok(lines[0].includes('"col2"'));
    });

    it('writes TSV header only when rows empty and columns provided', () => {
        const s = serializeRows([], 'tsv', false, ['h1', 'h2']);
        assert.ok(s.startsWith('\uFEFF'));
        const lines = s.trimEnd().split(/\r?\n/);
        assert.equal(lines.length, 1);
        assert.match(lines[0], /h1\th2/);
    });

    it('JSON ignores columns when rows empty', () => {
        const s = serializeRows([], 'json', false, ['a', 'b']);
        assert.equal(s.trim(), '[]');
    });
});

describe('fetchDataExtensionFieldNames', () => {
    it('sorts by Ordinal and returns Names', async () => {
        const soap = {
            retrieve: async (type, props, opts) => {
                assert.equal(type, 'DataExtensionField');
                assert.deepEqual(props, ['Name', 'Ordinal']);
                assert.equal(opts.filter.leftOperand, 'DataExtension.CustomerKey');
                assert.equal(opts.filter.operator, 'equals');
                assert.equal(opts.filter.rightOperand, 'MY_DE');
                return {
                    Results: [
                        { Name: 'second', Ordinal: '2' },
                        { Name: 'first', Ordinal: '1' },
                    ],
                };
            },
        };
        const names = await fetchDataExtensionFieldNames(soap, 'MY_DE');
        assert.deepEqual(names, ['first', 'second']);
    });

    it('returns empty array when Results missing or empty', async () => {
        assert.deepEqual(
            await fetchDataExtensionFieldNames({ retrieve: async () => ({ Results: [] }) }, 'K'),
            [],
        );
        assert.deepEqual(
            await fetchDataExtensionFieldNames({ retrieve: async () => ({}) }, 'K'),
            [],
        );
    });
});

describe('exportDataExtensionToFile', () => {
    it('writes header-only CSV for empty DE when SOAP returns fields', async () => {
        const tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-export-empty-'));
        try {
            const sdk = {
                rest: {
                    getBulk: async () => {
                        throw new Error('Could not find an array to iterate over');
                    },
                },
                soap: {
                    retrieve: async (type, props) => {
                        assert.equal(type, 'DataExtensionField');
                        assert.deepEqual(props, ['Name', 'Ordinal']);
                        return {
                            Results: [
                                { Name: 'Email', Ordinal: '1' },
                                { Name: 'Name', Ordinal: '2' },
                            ],
                        };
                    },
                },
            };
            const { path: outPath, rowCount } = await exportDataExtensionToFile(sdk, {
                projectRoot: tmp,
                credentialName: 'cred',
                buName: 'bu',
                deKey: 'DE_KEY',
                format: 'csv',
            });
            assert.equal(rowCount, 0);
            const body = await fs.readFile(outPath, 'utf8');
            assert.ok(body.startsWith('\uFEFF'));
            assert.ok(body.includes('"Email"'));
            assert.ok(body.includes('"Name"'));
            const lines = body.trimEnd().split(/\r?\n/);
            assert.equal(lines.length, 1);
        } finally {
            await fs.rm(tmp, { recursive: true, force: true });
        }
    });
});
