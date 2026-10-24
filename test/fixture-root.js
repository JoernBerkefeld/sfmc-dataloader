import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';

/**
 * Verify placement before creating any synthetic fixture.
 *
 * @param {object} environment
 * @param {string} temporaryDirectory
 * @param {typeof path} paths
 */
export function assertFixtureTemporary(
    environment = process.env,
    temporaryDirectory = os.tmpdir(),
    paths = path,
) {
    const normalize = (value) =>
        paths === path.win32 ? paths.resolve(value).toLowerCase() : paths.resolve(value);
    assert.ok(
        temporaryDirectory && paths.isAbsolute(temporaryDirectory),
        'OS temp directory must be absolute',
    );
    const expected = normalize(temporaryDirectory);
    assert.notEqual(expected, normalize(paths.parse(expected).root), 'OS temp must not be a root');
    for (const name of ['TEMP', 'TMP', 'TMPDIR']) {
        const value = environment[name];
        if (!value) {
            continue;
        }
        assert.ok(paths.isAbsolute(value), `${name} must be an absolute temp path`);
        assert.equal(normalize(value), expected, `${name} must resolve to the OS temp directory`);
    }
}

/**
 * Create an owned root; retain small synthetic fixtures for diagnostics, never delete it.
 *
 * @param {string} prefix
 */
export async function createFixtureRoot(prefix) {
    assertFixtureTemporary();
    assert.match(prefix, /^[a-z\d-]+-$/iu, 'Fixture prefix must be a basename prefix');
    const root = await fs.mkdtemp(path.join(os.tmpdir(), prefix));
    await fs.writeFile(path.join(root, '.fixture-owner'), randomUUID(), { flag: 'wx' });
    return root;
}
