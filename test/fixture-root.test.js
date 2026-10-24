import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { describe, it } from 'node:test';
import { assertFixtureTemporary, createFixtureRoot } from './fixture-root.js';

const environment = { TEMP: String.raw`F:\Temp`, TMP: 'f:\\temp\\', TMPDIR: '' };

describe('synthetic fixture roots', () => {
    it('accepts normalized Windows temp directories without requiring a machine-specific location', () => {
        assert.doesNotThrow(() => assertFixtureTemporary(environment, 'F:/Temp/', path.win32));
        assert.doesNotThrow(() =>
            assertFixtureTemporary(
                { TEMP: String.raw`C:\Users\runner\Temp` },
                'C:/Users/runner/Temp',
                path.win32,
            ),
        );
    });

    it('accepts POSIX OS temp directories without TEMP and preserves case sensitivity', () => {
        assert.doesNotThrow(() => assertFixtureTemporary({}, '/tmp', path.posix));
        assert.doesNotThrow(() =>
            assertFixtureTemporary({ TMPDIR: '/var/tmp/' }, '/var/tmp', path.posix),
        );
        assert.throws(() => assertFixtureTemporary({ TEMP: '/TMP' }, '/tmp', path.posix));
    });

    it('rejects mismatches, relative paths and bare drive roots before creation', () => {
        for (const name of ['TEMP', 'TMP', 'TMPDIR']) {
            for (const value of ['F:', 'F:\\', 'Temp', String.raw`C:\Temp`]) {
                assert.throws(() =>
                    assertFixtureTemporary(
                        { ...environment, [name]: value },
                        String.raw`F:\Temp`,
                        path.win32,
                    ),
                );
            }
        }
        for (const value of ['', 'F:', 'F:\\', 'Temp']) {
            assert.throws(() => assertFixtureTemporary({}, value, path.win32));
        }
        for (const value of ['', '/', '/tmp/..', 'tmp']) {
            assert.throws(() => assertFixtureTemporary({}, value, path.posix));
        }
        assert.throws(() => assertFixtureTemporary({ TMP: '/var/tmp' }, '/tmp', path.posix));
        assert.throws(() => assertFixtureTemporary({ TMPDIR: 'tmp' }, '/tmp', path.posix));
    });

    it('rejects path-like prefixes without creating a root', async () => {
        for (const prefix of ['', '../escape-', String.raw`F:\Temp\escape-`, 'nested/root-']) {
            await assert.rejects(createFixtureRoot(prefix), /basename prefix/u);
        }
    });

    it('creates unique retained roots with unguessable ownership markers', async () => {
        const first = await createFixtureRoot('mcdata-fixture-test-');
        const second = await createFixtureRoot('mcdata-fixture-test-');
        assert.notEqual(first, second);
        assert.equal(path.dirname(first), path.resolve(os.tmpdir()));
        const marker = await fs.readFile(path.join(first, '.fixture-owner'), 'utf8');
        assert.match(marker, /^[a-f\d-]{36}$/u);
        assert.notEqual(marker, await fs.readFile(path.join(second, '.fixture-owner'), 'utf8'));
        assert.ok((await fs.stat(first)).isDirectory());
    });
});
