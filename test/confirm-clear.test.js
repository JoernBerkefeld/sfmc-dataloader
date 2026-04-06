import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Readable, Writable } from 'node:stream';
import { confirmClearBeforeImport } from '../lib/confirm-clear.mjs';

describe('confirmClearBeforeImport', () => {
    it('skips prompt when acceptRiskFlag', async () => {
        await confirmClearBeforeImport({
            deKeys: ['A'],
            acceptRiskFlag: true,
            isTTY: false,
        });
    });

    it('throws in non-interactive mode without flag', async () => {
        await assert.rejects(
            () =>
                confirmClearBeforeImport({
                    deKeys: ['A'],
                    acceptRiskFlag: false,
                    isTTY: false,
                }),
            /non-interactive/
        );
    });

    it('reads YES from stdin in TTY mode', async () => {
        const input = Readable.from(['YES\n']);
        const chunks = [];
        const output = new Writable({
            write(chunk, _enc, cb) {
                chunks.push(chunk);
                cb();
            },
        });
        await confirmClearBeforeImport({
            deKeys: ['K'],
            acceptRiskFlag: false,
            isTTY: true,
            stdin: input,
            stdout: output,
        });
    });
});
