import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const bin = path.join(__dirname, '..', 'bin', 'mcdata.mjs');

describe('mcdata CLI', () => {
    it('prints help with -h', () => {
        const r = spawnSync(process.execPath, [bin, '-h'], { encoding: 'utf8' });
        assert.equal(r.status, 0);
        assert.ok(r.stdout.includes('mcdata'));
    });

    it('fails with no args', () => {
        const r = spawnSync(process.execPath, [bin], { encoding: 'utf8' });
        assert.ok(r.status !== 0);
    });
});
