import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { Readable, Writable } from 'node:stream';
import { confirmClearBeforeImport } from '../lib/confirm-clear.mjs';

function makeOutput() {
    const chunks = [];
    const stream = new Writable({
        write(chunk, _enc, cb) {
            chunks.push(String(chunk));
            cb();
        },
    });
    return { stream, text: () => chunks.join('') };
}

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
            /non-interactive/,
        );
    });

    it('reads YES from stdin in TTY mode', async () => {
        const input = Readable.from(['YES\n']);
        const { stream: output } = makeOutput();
        await confirmClearBeforeImport({
            deKeys: ['K'],
            acceptRiskFlag: false,
            isTTY: true,
            stdin: input,
            stdout: output,
        });
    });

    it('shows single-BU warning when no targets provided', async () => {
        const input = Readable.from(['YES\n']);
        const { stream: output, text } = makeOutput();
        await confirmClearBeforeImport({
            deKeys: ['DE_A', 'DE_B'],
            acceptRiskFlag: false,
            isTTY: true,
            stdin: input,
            stdout: output,
        });
        const msg = text();
        assert.ok(msg.includes('DE_A'), 'should list DE_A');
        assert.ok(msg.includes('DE_B'), 'should list DE_B');
        assert.ok(
            !msg.includes('Business Unit'),
            'single-BU mode should not mention Business Unit count',
        );
    });

    it('shows multi-BU warning when targets array is provided', async () => {
        const input = Readable.from(['YES\n']);
        const { stream: output, text } = makeOutput();
        const targets = [
            { credential: 'MyCred', bu: 'QA' },
            { credential: 'MyCred', bu: 'Prod' },
        ];
        await confirmClearBeforeImport({
            deKeys: ['DE_A'],
            targets,
            acceptRiskFlag: false,
            isTTY: true,
            stdin: input,
            stdout: output,
        });
        const msg = text();
        assert.ok(msg.includes('2 Business Unit'), 'should mention 2 business units');
        assert.ok(msg.includes('MyCred/QA'), 'should list QA target');
        assert.ok(msg.includes('MyCred/Prod'), 'should list Prod target');
        assert.ok(msg.includes('DE_A'), 'should list the DE key under each BU');
    });

    it('aborts when user does not type YES', async () => {
        const input = Readable.from(['no\n']);
        const { stream: output } = makeOutput();
        await assert.rejects(
            () =>
                confirmClearBeforeImport({
                    deKeys: ['K'],
                    acceptRiskFlag: false,
                    isTTY: true,
                    stdin: input,
                    stdout: output,
                }),
            /Aborted/,
        );
    });
});
