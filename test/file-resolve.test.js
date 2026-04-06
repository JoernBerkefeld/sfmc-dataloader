import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { findImportCandidates, pickLatestByMtime } from '../lib/file-resolve.mjs';
import { buildExportBasename } from '../lib/filename.mjs';

describe('file-resolve', () => {
    let tmp;
    before(async () => {
        tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-fr-'));
        const key = 'TestDE';
        const older = buildExportBasename(key, '2020-01-01T00-00-00.000Z', 'csv');
        const newer = buildExportBasename(key, '2026-01-01T00-00-00.000Z', 'csv');
        await fs.writeFile(path.join(tmp, older), 'a', 'utf8');
        await new Promise((r) => setTimeout(r, 20));
        await fs.writeFile(path.join(tmp, newer), 'b', 'utf8');
    });

    after(async () => {
        await fs.rm(tmp, { recursive: true, force: true });
    });

    it('findImportCandidates returns matching files', async () => {
        const found = await findImportCandidates(tmp, 'TestDE', 'csv');
        assert.equal(found.length, 2);
    });

    it('pickLatestByMtime picks newest', async () => {
        const found = await findImportCandidates(tmp, 'TestDE', 'csv');
        const best = await pickLatestByMtime(found);
        assert.ok(best.includes('2026-01-01'));
    });
});
