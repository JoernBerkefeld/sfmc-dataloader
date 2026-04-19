import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { log } from '../lib/log.mjs';

describe('log', () => {
    it('prefixes info, warn, and error with HH:MM:SS and level', () => {
        /* eslint-disable no-console */
        const stdoutLines = [];
        const stderrLines = [];
        const origLog = console.log;
        const origErr = console.error;
        const origWarn = console.warn;
        console.log = (s) => {
            stdoutLines.push(String(s));
        };
        console.error = (s) => {
            stderrLines.push(String(s));
        };
        console.warn = (s) => {
            stderrLines.push(String(s));
        };
        try {
            log.info('hello');
            log.warn('careful');
            log.error('oops');
        } finally {
            console.log = origLog;
            console.error = origErr;
            console.warn = origWarn;
        }
        /* eslint-enable no-console */

        // info goes to stdout, warn and error go to stderr
        assert.equal(stdoutLines.length, 1);
        assert.equal(stderrLines.length, 2);

        assert.ok(/^\d{2}:\d{2}:\d{2} info: hello$/.test(stdoutLines[0]), stdoutLines[0]);
        assert.ok(/^\d{2}:\d{2}:\d{2} warn: careful$/.test(stderrLines[0]), stderrLines[0]);
        assert.ok(/^\d{2}:\d{2}:\d{2} error: oops$/.test(stderrLines[1]), stderrLines[1]);
    });
});
