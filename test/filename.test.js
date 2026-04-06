import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    filterIllegalFilenames,
    reverseFilterIllegalFilenames,
    buildExportBasename,
    parseExportBasename,
    MCDATA_SENTINEL,
} from '../lib/filename.mjs';

describe('filename', () => {
    it('round-trips filter / reverse like mcdev', () => {
        const raw = 'foo*bar{baz}[qux]@v1';
        const f = filterIllegalFilenames(raw);
        assert.equal(reverseFilterIllegalFilenames(f), raw);
    });

    it('does not produce +MCDATA+ inside encoded key segment', () => {
        const key = 'a+b';
        const base = buildExportBasename(key, '2026-04-06T12-00-00.000Z', 'csv');
        assert.ok(!base.split(MCDATA_SENTINEL)[0].includes(MCDATA_SENTINEL));
        assert.ok(base.includes(MCDATA_SENTINEL));
    });

    it('parseExportBasename recovers customer key', () => {
        const key = 'My_DE_Key';
        const ts = '2026-04-06T12-00-00.000Z';
        const base = buildExportBasename(key, ts, 'csv');
        const { customerKey, ext } = parseExportBasename(base);
        assert.equal(customerKey, key);
        assert.equal(ext, 'csv');
    });
});
