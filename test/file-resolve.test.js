import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import {
    findImportCandidates,
    formatFromExtension,
    pickLatestByMtime,
} from '../lib/file-resolve.mjs';
import { buildExportBasename } from '../lib/filename.mjs';

describe('formatFromExtension', () => {
    it('returns csv for .csv files', () => {
        assert.equal(formatFromExtension('data/file.csv'), 'csv');
        assert.equal(formatFromExtension('file.CSV'), 'csv');
    });

    it('returns tsv for .tsv files', () => {
        assert.equal(formatFromExtension('data/file.tsv'), 'tsv');
        assert.equal(formatFromExtension('file.TSV'), 'tsv');
    });

    it('returns json for .json files', () => {
        assert.equal(formatFromExtension('data/file.json'), 'json');
        assert.equal(formatFromExtension('file.JSON'), 'json');
    });

    it('returns null for unsupported extensions', () => {
        assert.equal(formatFromExtension('file.txt'), null);
        assert.equal(formatFromExtension('file.xml'), null);
        assert.equal(formatFromExtension('file'), null);
    });
});

describe('file-resolve', () => {
    let tmp;
    before(async () => {
        tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-fr-'));
        const key = 'TestDE';
        const olderCsv = buildExportBasename(key, '2020-01-01T00-00-00.000Z', 'csv');
        const newerCsv = buildExportBasename(key, '2026-01-01T00-00-00.000Z', 'csv');
        const tsvFile = buildExportBasename(key, '2025-06-01T00-00-00.000Z', 'tsv');
        await fs.writeFile(path.join(tmp, olderCsv), 'a', 'utf8');
        await new Promise((r) => setTimeout(r, 20));
        await fs.writeFile(path.join(tmp, tsvFile), 'c', 'utf8');
        await new Promise((r) => setTimeout(r, 20));
        await fs.writeFile(path.join(tmp, newerCsv), 'b', 'utf8');
    });

    after(async () => {
        await fs.rm(tmp, { recursive: true, force: true });
    });

    it('findImportCandidates returns matching files for specific format', async () => {
        const found = await findImportCandidates(tmp, 'TestDE', 'csv');
        assert.equal(found.length, 2);
        assert.ok(found.every((f) => f.endsWith('.csv')));
    });

    it('findImportCandidates searches all formats when format omitted', async () => {
        const found = await findImportCandidates(tmp, 'TestDE');
        assert.equal(found.length, 3);
        assert.ok(found.some((f) => f.endsWith('.csv')));
        assert.ok(found.some((f) => f.endsWith('.tsv')));
    });

    it('pickLatestByMtime picks newest', async () => {
        const found = await findImportCandidates(tmp, 'TestDE', 'csv');
        const best = await pickLatestByMtime(found);
        assert.ok(best.includes('2026-01-01'));
    });
});
