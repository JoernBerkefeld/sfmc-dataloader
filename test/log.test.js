import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { log } from '../lib/log.mjs';

describe('log', () => {
    it('prefixes info, warn, and error with HH:MM:SS and level', () => {
        const lines = [];
        const origErr = console.error;
        const origWarn = console.warn;
        console.error = (s) => {
            lines.push(String(s));
        };
        console.warn = (s) => {
            lines.push(String(s));
        };
        try {
            log.info('hello');
            log.warn('careful');
            log.error('oops');
        } finally {
            console.error = origErr;
            console.warn = origWarn;
        }
        assert.equal(lines.length, 3);
        assert.ok(/^\d{2}:\d{2}:\d{2} info: hello$/.test(lines[0]), lines[0]);
        assert.ok(/^\d{2}:\d{2}:\d{2} warn: careful$/.test(lines[1]), lines[1]);
        assert.ok(/^\d{2}:\d{2}:\d{2} error: oops$/.test(lines[2]), lines[2]);
    });
});
