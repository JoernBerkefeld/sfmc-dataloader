import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';

/**
 * @typedef {{ credential: string, bu: string }} CredBuTarget
 */

/**
 * @param {object} options
 * @param {string[]} options.deKeys
 * @param {boolean} options.acceptRiskFlag
 * @param {boolean} options.isTTY
 * @param {CredBuTarget[]} [options.targets] When present, renders a per-BU breakdown.
 * @param {NodeJS.ReadableStream} [options.stdin]
 * @param {NodeJS.WritableStream} [options.stdout]
 * @returns {Promise.<void>}
 */
export async function confirmClearBeforeImport(options) {
    const { deKeys, targets, acceptRiskFlag, isTTY } = options;
    if (acceptRiskFlag) {
        return;
    }
    if (!isTTY) {
        throw new Error(
            'Refusing to clear data in non-interactive mode without --i-accept-clear-data-risk. ' +
                'All rows in the target Data Extension(s) would be permanently deleted.',
        );
    }
    const stdin = options.stdin ?? input;
    const stdout = options.stdout ?? output;
    const body =
        targets && targets.length > 0
            ? `This will permanently DELETE ALL ROWS across ${targets.length} Business Unit(s):\n\n` +
              targets
                  .map(
                      ({ credential, bu }) =>
                          `  ${credential}/${bu}:\n` + deKeys.map((k) => `    - ${k}\n`).join(''),
                  )
                  .join('') +
              '\nThis cannot be undone. Enterprise 2.0 / admin / shared-DE rules may apply.\n'
            : 'This will permanently DELETE ALL ROWS in:\n' +
              deKeys.map((k) => `  - ${k}\n`).join('') +
              'This cannot be undone. Enterprise 2.0 / admin / shared-DE rules may apply.\n';
    const message =
        '\n*** DANGER: CLEAR DATA ***\n' + body + 'Type YES to continue, anything else to abort: ';
    stdout.write(message);
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
