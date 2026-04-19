import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
    countDataRowsFromImportPaths,
    readRowsFromFile,
    readRowsFromImportPaths,
    streamRowsFromFile,
    streamRowsFromImportPaths,
} from '../lib/read-rows.mjs';

/**
 * @param {AsyncIterable<object>} gen
 * @returns {Promise.<object[]>}
 */
async function collectAsync(gen) {
    const out = [];
    for await (const row of gen) {
        out.push(row);
    }
    return out;
}

describe('readRowsFromFile', () => {
    let tmp;
    before(async () => {
        tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-read-'));
    });
    after(async () => {
        await fs.rm(tmp, { recursive: true, force: true });
    });

    it('parses JSON array', async () => {
        const p = path.join(tmp, 'a.json');
        await fs.writeFile(p, JSON.stringify([{ a: 1 }]), 'utf8');
        const rows = await readRowsFromFile(p, 'json');
        assert.deepEqual(rows, [{ a: 1 }]);
    });

    it('parses JSON items wrapper', async () => {
        const p = path.join(tmp, 'b.json');
        await fs.writeFile(p, JSON.stringify({ items: [{ b: 2 }] }), 'utf8');
        const rows = await readRowsFromFile(p, 'json');
        assert.deepEqual(rows, [{ b: 2 }]);
    });

    it('reads CSV rows', async () => {
        const p = path.join(tmp, 'c.csv');
        await fs.writeFile(p, 'col1,col2\n1,2\n', 'utf8');
        const rows = await readRowsFromFile(p, 'csv');
        assert.deepEqual(rows, [{ col1: '1', col2: '2' }]);
    });

    it('reads TSV rows', async () => {
        const p = path.join(tmp, 'd.tsv');
        await fs.writeFile(p, 'col1\tcol2\n1\t2\n', 'utf8');
        const rows = await readRowsFromFile(p, 'tsv');
        assert.deepEqual(rows, [{ col1: '1', col2: '2' }]);
    });

    it('strips BOM from first header field in TSV', async () => {
        const p = path.join(tmp, 'bom.tsv');
        const bom = '\uFEFF';
        await fs.writeFile(p, `${bom}col1\tcol2\n1\t2\n`, 'utf8');
        const rows = await readRowsFromFile(p, 'tsv');
        assert.deepEqual(rows, [{ col1: '1', col2: '2' }]);
        assert.ok(!Object.keys(rows[0]).some((k) => k.includes('\uFEFF')), 'no BOM in keys');
    });

    it('strips surrounding quotes from TSV headers and values', async () => {
        const p = path.join(tmp, 'quoted.tsv');
        await fs.writeFile(p, '"col1"\t"col2"\n"val1"\t"val2"\n', 'utf8');
        const rows = await readRowsFromFile(p, 'tsv');
        assert.deepEqual(rows, [{ col1: 'val1', col2: 'val2' }]);
    });

    it('handles BOM + quoted headers in TSV', async () => {
        const p = path.join(tmp, 'bom-quoted.tsv');
        const bom = '\uFEFF';
        await fs.writeFile(p, `${bom}"col1"\t"col2"\n"v1"\t"v2"\n`, 'utf8');
        const rows = await readRowsFromFile(p, 'tsv');
        assert.deepEqual(rows, [{ col1: 'v1', col2: 'v2' }]);
        assert.ok(!Object.keys(rows[0]).some((k) => k.includes('\uFEFF')), 'no BOM in keys');
        assert.ok(!Object.keys(rows[0]).some((k) => k.includes('"')), 'no quotes in keys');
    });

    it('reads CSV continuation without header when columnHeaders is provided', async () => {
        const p = path.join(tmp, 'cont.csv');
        await fs.writeFile(p, '3,4\n', 'utf8');
        const rows = await readRowsFromFile(p, 'csv', {
            columnHeaders: ['col1', 'col2'],
        });
        assert.deepEqual(rows, [{ col1: '3', col2: '4' }]);
    });
});

describe('streamRowsFromFile', () => {
    let tmp;
    before(async () => {
        tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-stream-'));
    });
    after(async () => {
        await fs.rm(tmp, { recursive: true, force: true });
    });

    it('matches readRowsFromFile for CSV', async () => {
        const p = path.join(tmp, 's.csv');
        await fs.writeFile(p, 'col1,col2\n1,2\n', 'utf8');
        const expected = await readRowsFromFile(p, 'csv');
        const streamed = await collectAsync(streamRowsFromFile(p, 'csv'));
        assert.deepEqual(streamed, expected);
    });

    it('matches readRowsFromFile for JSON', async () => {
        const p = path.join(tmp, 's.json');
        await fs.writeFile(p, JSON.stringify([{ x: 1 }]), 'utf8');
        const expected = await readRowsFromFile(p, 'json');
        const streamed = await collectAsync(streamRowsFromFile(p, 'json'));
        assert.deepEqual(streamed, expected);
    });
});

describe('readRowsFromImportPaths', () => {
    let tmp;
    before(async () => {
        tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-read-multi-'));
    });
    after(async () => {
        await fs.rm(tmp, { recursive: true, force: true });
    });

    it('concatenates two CSV parts (header in first file only)', async () => {
        const p1 = path.join(tmp, 'a.csv');
        const p2 = path.join(tmp, 'b.csv');
        await fs.writeFile(p1, 'col1,col2\n1,2\n', 'utf8');
        await fs.writeFile(p2, '3,4\n', 'utf8');
        const rows = await readRowsFromImportPaths([p1, p2], 'csv');
        assert.deepEqual(rows, [
            { col1: '1', col2: '2' },
            { col1: '3', col2: '4' },
        ]);
    });

    it('concatenates JSON arrays from multiple files', async () => {
        const p1 = path.join(tmp, 'x.json');
        const p2 = path.join(tmp, 'y.json');
        await fs.writeFile(p1, JSON.stringify([{ a: 1 }]), 'utf8');
        await fs.writeFile(p2, JSON.stringify([{ a: 2 }]), 'utf8');
        const rows = await readRowsFromImportPaths([p1, p2], 'json');
        assert.deepEqual(rows, [{ a: 1 }, { a: 2 }]);
    });

    it('streamRowsFromImportPaths matches readRowsFromImportPaths for multi-part CSV', async () => {
        const p1 = path.join(tmp, 'sa.csv');
        const p2 = path.join(tmp, 'sb.csv');
        await fs.writeFile(p1, 'col1,col2\n1,2\n', 'utf8');
        await fs.writeFile(p2, '3,4\n', 'utf8');
        const expected = await readRowsFromImportPaths([p1, p2], 'csv');
        const streamed = await collectAsync(streamRowsFromImportPaths([p1, p2], 'csv'));
        assert.deepEqual(streamed, expected);
    });

    it('countDataRowsFromImportPaths matches row array length', async () => {
        const p1 = path.join(tmp, 'cnt.csv');
        await fs.writeFile(p1, 'a,b\n1,2\n3,4\n', 'utf8');
        const rows = await readRowsFromImportPaths([p1], 'csv');
        const n = await countDataRowsFromImportPaths([p1], 'csv');
        assert.equal(n, rows.length);
    });
});
