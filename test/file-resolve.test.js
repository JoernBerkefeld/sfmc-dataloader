import assert from 'node:assert/strict';
import { describe, it, before } from 'node:test';
import fs from 'node:fs/promises';
import { createFixtureRoot } from './fixture-root.js';
import path from 'node:path';
import {
    findImportCandidates,
    formatFromExtension,
    pickLatestByMtime,
    resolveImportSet,
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
    let temporary;
    before(async () => {
        temporary = await createFixtureRoot('mcdata-fr-');
        const key = 'TestDE';
        const olderCsv = buildExportBasename(key, '2020-01-01T00-00-00.000Z', 'csv');
        const newerCsv = buildExportBasename(key, '2026-01-01T00-00-00.000Z', 'csv');
        const tsvFile = buildExportBasename(key, '2025-06-01T00-00-00.000Z', 'tsv');
        await fs.writeFile(path.join(temporary, olderCsv), 'a', 'utf8');
        await new Promise((r) => setTimeout(r, 20));
        await fs.writeFile(path.join(temporary, tsvFile), 'c', 'utf8');
        await new Promise((r) => setTimeout(r, 20));
        await fs.writeFile(path.join(temporary, newerCsv), 'b', 'utf8');
    });

    it('findImportCandidates returns matching files for specific format', async () => {
        const found = await findImportCandidates(temporary, 'TestDE', 'csv');
        assert.equal(found.length, 2);
        assert.ok(found.every((f) => f.endsWith('.csv')));
    });

    it('findImportCandidates searches all formats when format omitted', async () => {
        const found = await findImportCandidates(temporary, 'TestDE');
        assert.equal(found.length, 3);
        assert.ok(found.some((f) => f.endsWith('.csv')));
        assert.ok(found.some((f) => f.endsWith('.tsv')));
    });

    it('pickLatestByMtime picks newest', async () => {
        const found = await findImportCandidates(temporary, 'TestDE', 'csv');
        const best = await pickLatestByMtime(found);
        assert.ok(best.includes('2026-01-01'));
    });
});

describe('resolveImportSet', () => {
    let temporary;
    before(async () => {
        temporary = await createFixtureRoot('mcdata-ris-');
    });
    /**
     * Create a synthetic export with a deterministic modification time.
     *
     * @param {string} key
     * @param {number|undefined} part
     * @param {number} [mtime]
     * @param {boolean} [isGitMode]
     * @returns {Promise.<string>}
     */
    async function fixture(key, part, mtime = 100, isGitMode = false) {
        const filePath = path.join(
            temporary,
            buildExportBasename(key, '2026-02-01T00-00-00.000Z', 'csv', isGitMode, part),
        );
        await fs.writeFile(filePath, 'field\nvalue\n', 'utf8');
        await fs.utimes(filePath, mtime, mtime);
        return filePath;
    }

    it('returns an empty set for no recognizable candidates', async () => {
        assert.deepEqual(await resolveImportSet([]), { paths: [], isMultiPart: false });
        assert.deepEqual(await resolveImportSet([path.join(temporary, 'unrelated.csv')]), {
            paths: [],
            isMultiPart: false,
        });
    });

    it('accepts single unnumbered and single part1 timestamped or git exports', async () => {
        for (const isGitMode of [false, true]) {
            for (const part of [undefined, 1]) {
                const filePath = await fixture(`Single-${isGitMode}-${part}`, part, 100, isGitMode);
                assert.deepEqual(await resolveImportSet([filePath]), {
                    paths: [filePath],
                    isMultiPart: false,
                });
            }
        }
    });

    it('rejects selected sets with a missing first or middle part', async () => {
        for (const parts of [[2], [1, 3], [0], [1, 2, 4]]) {
            const paths = await Promise.all(
                parts.map((part) => fixture(`Gap-${parts.join('-')}`, part)),
            );
            await assert.rejects(resolveImportSet(paths), /contiguous parts starting at 1/);
        }
    });

    it('rejects duplicate numbered and unnumbered members', async () => {
        for (const part of [undefined, 1, 2]) {
            const p1 = await fixture(`Duplicate-${part}`, 1);
            const duplicate = await fixture(`Duplicate-${part}`, part);
            const paths = part === 2 ? [p1, duplicate, duplicate] : [duplicate, duplicate];
            await assert.rejects(resolveImportSet(paths), /Duplicate .*selected import set/);
        }
    });

    it('rejects selected sets mixing numbered and unnumbered exports', async () => {
        const plain = await fixture('Mixed');
        const numbered = await fixture('Mixed', 1);
        await assert.rejects(resolveImportSet([plain, numbered]), /mixes numbered and unnumbered/);
    });

    it('rejects explicit selection across runs or formats even when mtimes differ', async () => {
        const first = await fixture('Explicit');
        for (const [timestamp, format] of [
            ['2027-01-01', 'csv'],
            ['2026-02-01T00-00-00.000Z', 'tsv'],
            ['', 'csv'],
        ]) {
            const other = path.join(
                temporary,
                buildExportBasename('Explicit', timestamp, format, timestamp === ''),
            );
            await fs.writeFile(other, 'field\nvalue\n');
            await fs.utimes(other, 200, 200);
            await assert.rejects(
                resolveImportSet([first, other], { explicitSelection: true }),
                /Ambiguous explicit import selection/,
            );
            assert.deepEqual((await resolveImportSet([first, other])).paths, [other]);
        }
    });

    it('preserves explicit contiguous multipart order and rejects gaps', async () => {
        const first = await fixture('ExplicitParts', 1);
        const second = await fixture('ExplicitParts', 2);
        assert.deepEqual(await resolveImportSet([second, first], { explicitSelection: true }), {
            paths: [first, second],
            isMultiPart: true,
        });
        await assert.rejects(
            resolveImportSet([second], { explicitSelection: true }),
            /contiguous parts starting at 1/,
        );
    });

    it('preserves maximum-mtime selection rather than timestamp ordering', async () => {
        const olderName = await fixture('Latest', 1, 300);
        const newerName = path.join(temporary, buildExportBasename('Latest', '2027-01-01', 'csv'));
        await fs.writeFile(newerName, 'field\nvalue\n');
        await fs.utimes(newerName, 200, 200);
        assert.deepEqual(await resolveImportSet([newerName, olderName]), {
            paths: [olderName],
            isMultiPart: false,
        });
    });

    it('preserves first-group selection when maximum mtimes tie', async () => {
        const first = await fixture('TieFirst');
        const second = await fixture('TieSecond');
        assert.deepEqual((await resolveImportSet([first, second])).paths, [first]);
    });

    it('does not validate incomplete or mixed unrelated historical groups', async () => {
        const gap = await fixture('History', 3, 100);
        const plain = await fixture('History', undefined, 100);
        const selected = await fixture('Current', undefined, 200);
        assert.deepEqual((await resolveImportSet([gap, plain, gap, selected])).paths, [selected]);
    });

    it('rejects a failed-stat member of the selected group instead of dropping it', async () => {
        const p1 = await fixture('Vanished', 1, 300);
        const p2 = path.join(
            temporary,
            buildExportBasename('Vanished', '2026-02-01T00-00-00.000Z', 'csv', false, 2),
        );
        const historical = await fixture('Fallback', undefined, 100);
        await assert.rejects(resolveImportSet([historical, p2, p1]), (error) => {
            assert.match(error.message, /Cannot stat selected import file/);
            assert.ok(error.message.includes(p2));
            assert.equal(error.cause.code, 'ENOENT');
            return true;
        });
    });

    it('rejects wholly missing candidates but ignores unstatable unrelated history', async () => {
        const missing = path.join(temporary, buildExportBasename('Missing', '', 'csv', true));
        await assert.rejects(resolveImportSet([missing]), /Cannot stat import candidate/);
        const selected = await fixture('Existing');
        assert.deepEqual((await resolveImportSet([missing, selected])).paths, [selected]);
    });

    it('rejects a selected path that is not a regular file', async () => {
        const directory = path.join(temporary, buildExportBasename('Directory', '', 'csv', true));
        await fs.mkdir(directory);
        await assert.rejects(resolveImportSet([directory]), /Cannot read selected import file/);
    });

    it('propagates selected open failures without opening historical files', async (t) => {
        const historical = await fixture('UnreadableHistory', undefined, 100);
        const selected = await fixture('UnreadableSelected', undefined, 200);
        const opened = [];
        t.mock.method(fs, 'open', async (filePath) => {
            opened.push(filePath);
            throw Object.assign(new Error('Synthetic permission denial'), { code: 'EACCES' });
        });
        await assert.rejects(resolveImportSet([historical, selected]), (error) => {
            assert.match(error.message, /Cannot read selected import file/);
            assert.ok(error.message.includes(selected));
            assert.equal(error.cause.code, 'EACCES');
            return true;
        });
        assert.deepEqual(opened, [selected]);
    });

    it('orders multi-part CSV paths by part number', async () => {
        const key = 'PartDE';
        const ts = '2026-02-01T00-00-00.000Z';
        const p2 = buildExportBasename(key, ts, 'csv', false, 2);
        const p1 = buildExportBasename(key, ts, 'csv', false, 1);
        await fs.writeFile(path.join(temporary, p2), 'x', 'utf8');
        await new Promise((r) => setTimeout(r, 10));
        await fs.writeFile(path.join(temporary, p1), 'y', 'utf8');
        const { paths, isMultiPart } = await resolveImportSet([
            path.join(temporary, p2),
            path.join(temporary, p1),
        ]);
        assert.equal(isMultiPart, true);
        assert.equal(paths.length, 2);
        assert.ok(paths[0].includes('part1'));
        assert.ok(paths[1].includes('part2'));
    });
});
