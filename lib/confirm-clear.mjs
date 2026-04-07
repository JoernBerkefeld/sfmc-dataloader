import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';

/**
 * @typedef {{ credential: string, bu: string }} CredBuTarget
 */

/**
 * @param {object} opts
 * @param {string[]} opts.deKeys
 * @param {boolean} opts.acceptRiskFlag
 * @param {boolean} opts.isTTY
 * @param {CredBuTarget[]} [opts.targets]  When present, renders a per-BU breakdown.
 * @param {NodeJS.ReadableStream} [opts.stdin]
 * @param {NodeJS.WritableStream} [opts.stdout]
 * @returns {Promise<void>}
 */
export async function confirmClearBeforeImport(opts) {
    const { deKeys, targets, acceptRiskFlag, isTTY } = opts;
    const stdin = opts.stdin ?? input;
    const stdout = opts.stdout ?? output;
    if (acceptRiskFlag) {
        return;
    }
    if (!isTTY) {
        throw new Error(
            'Refusing to clear data in non-interactive mode without --i-accept-clear-data-risk. ' +
                'All rows in the target Data Extension(s) would be permanently deleted.'
        );
    }
    let msg;
    if (targets && targets.length > 0) {
        msg =
            '\n*** DANGER: CLEAR DATA ***\n' +
            `This will permanently DELETE ALL ROWS across ${targets.length} Business Unit(s):\n\n` +
            targets
                .map(
                    ({ credential, bu }) =>
                        `  ${credential}/${bu}:\n` + deKeys.map((k) => `    - ${k}\n`).join('')
                )
                .join('') +
            '\nThis cannot be undone. Enterprise 2.0 / admin / shared-DE rules may apply.\n' +
            'Type YES to continue, anything else to abort: ';
    } else {
        msg =
            '\n*** DANGER: CLEAR DATA ***\n' +
            'This will permanently DELETE ALL ROWS in:\n' +
            deKeys.map((k) => `  - ${k}\n`).join('') +
            'This cannot be undone. Enterprise 2.0 / admin / shared-DE rules may apply.\n' +
            'Type YES to continue, anything else to abort: ';
    }
    stdout.write(msg);
    const rl = readline.createInterface({ input: stdin, output: stdout });
    try {
        const line = await rl.question('');
        if (line.trim() !== 'YES') {
            throw new Error('Aborted by user (clear not confirmed).');
        }
    } finally {
        rl.close();
    }
}
