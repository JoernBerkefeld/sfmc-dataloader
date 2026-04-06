import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { readRowsFromFile } from '../lib/read-rows.mjs';

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
});
