import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    filterIllegalFilenames,
    reverseFilterIllegalFilenames,
    buildExportBasename,
    parseExportBasename,
    MCDATA_SEGMENT,
} from '../lib/filename.mjs';

describe('filename', () => {
    it('round-trips filter / reverse like mcdev', () => {
        const raw = 'foo*bar{baz}[qux]@v1';
        const f = filterIllegalFilenames(raw);
        assert.equal(reverseFilterIllegalFilenames(f), raw);
    });

    it('uses .mcdata. segment between key and timestamp', () => {
        const key = 'a+b';
        const base = buildExportBasename(key, '2026-04-06T12-00-00.000Z', 'csv');
        assert.ok(base.includes(MCDATA_SEGMENT));
        assert.ok(!base.split(MCDATA_SEGMENT)[0].includes('mcdata'));
    });

    it('parseExportBasename recovers customer key (timestamped)', () => {
        const key = 'My_DE_Key';
        const ts = '2026-04-06T12-00-00.000Z';
        const base = buildExportBasename(key, ts, 'csv');
        const { customerKey, ext, timestampPart } = parseExportBasename(base);
        assert.equal(customerKey, key);
        assert.equal(ext, 'csv');
        assert.equal(timestampPart, ts);
    });

    it('buildExportBasename --git produces stable basename', () => {
        const base = buildExportBasename('MyDE', 'ignored', 'csv', true);
        assert.equal(base, `${filterIllegalFilenames('MyDE')}.mcdata.csv`);
        const { customerKey, timestampPart } = parseExportBasename(base);
        assert.equal(customerKey, 'MyDE');
        assert.equal(timestampPart, '');
    });

    it('parseExportBasename allows dots inside timestamp segment', () => {
        const enc = filterIllegalFilenames('K');
        const stem = `${enc}.mcdata.part.two`;
        const { timestampPart } = parseExportBasename(`${stem}.csv`);
        assert.equal(timestampPart, 'part.two');
    });

    it('buildExportBasename encodes multipart before timestamp', () => {
        const base = buildExportBasename('DE', '2026-01-01T00-00-00.000Z', 'csv', false, 3);
        const parsed = parseExportBasename(base);
        assert.equal(parsed.customerKey, 'DE');
        assert.equal(parsed.partNumber, 3);
        assert.equal(parsed.timestampPart, '2026-01-01T00-00-00.000Z');
    });
});
