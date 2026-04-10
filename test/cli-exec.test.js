import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { describe, it } from 'node:test';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const bin = path.join(__dirname, '..', 'bin', 'mcdata.mjs');
const packageJson = JSON.parse(readFileSync(path.join(__dirname, '..', 'package.json'), 'utf8'));

describe('mcdata CLI', () => {
    it('prints bare semver with --version', () => {
        const r = spawnSync(process.execPath, [bin, '--version'], { encoding: 'utf8' });
        assert.equal(r.status, 0);
        assert.equal(r.stdout.trim(), packageJson.version);
    });

    it('prints help with -h', () => {
        const r = spawnSync(process.execPath, [bin, '-h'], { encoding: 'utf8' });
        assert.equal(r.status, 0);
        assert.ok(r.stdout.includes('mcdata'));
    });

    it('fails with no args', () => {
        const r = spawnSync(process.execPath, [bin], { encoding: 'utf8' });
        assert.ok(r.status !== 0);
    });

    it('help includes --to + --file usage line', () => {
        const r = spawnSync(process.execPath, [bin, '-h'], { encoding: 'utf8' });
        assert.equal(r.status, 0);
        assert.ok(r.stdout.includes('--to'), 'help should mention --to flag');
        assert.ok(r.stdout.includes('--file'), 'help should mention --file flag');
    });

    it('help mentions --git and import --mode', () => {
        const r = spawnSync(process.execPath, [bin, '-h'], { encoding: 'utf8' });
        assert.equal(r.status, 0);
        assert.ok(r.stdout.includes('--git'));
        assert.ok(r.stdout.includes('--mode'));
    });

    it('help mentions --debug with log file info', () => {
        const r = spawnSync(process.execPath, [bin, '-h'], { encoding: 'utf8' });
        assert.equal(r.status, 0);
        assert.ok(r.stdout.includes('--debug'), 'help should mention --debug flag');
        assert.ok(r.stdout.includes('./logs/data/'), 'help should mention log directory');
    });

    it('help indicates format is auto-detected for imports', () => {
        const r = spawnSync(process.execPath, [bin, '-h'], { encoding: 'utf8' });
        assert.equal(r.status, 0);
        assert.ok(
            r.stdout.includes('auto-detected from file extension'),
            'help should mention format auto-detection',
        );
        assert.ok(
            r.stdout.includes('ignored for imports'),
            'help should indicate --format is ignored for imports',
        );
    });
});

describe('mcdata CLI — import --to + --file validation', () => {
    it('rejects --to + --file when a positional cred/bu is also given', () => {
        const r = spawnSync(
            process.execPath,
            [bin, 'import', 'MyCred/MyBU', '--to', 'MyCred/QA', '--file', 'some.csv'],
            { encoding: 'utf8' },
        );
        assert.notEqual(r.status, 0);
        assert.ok(r.stderr.includes('positional') || r.stderr.includes('Cannot mix'));
    });

    it('rejects --to + --file when --de is also provided', () => {
        const r = spawnSync(
            process.execPath,
            [bin, 'import', '--to', 'MyCred/QA', '--file', 'some.csv', '--de', 'DE1'],
            { encoding: 'utf8' },
        );
        assert.notEqual(r.status, 0);
        assert.ok(r.stderr.includes('--de') || r.stderr.includes('Cannot mix'));
    });

    it('rejects --from + --to + --file (mixed API and file mode)', () => {
        const r = spawnSync(
            process.execPath,
            [bin, 'import', '--from', 'MyCred/Dev', '--to', 'MyCred/QA', '--file', 'some.csv'],
            { encoding: 'utf8' },
        );
        assert.notEqual(r.status, 0);
        assert.ok(r.stderr.includes('--file'));
    });
});
