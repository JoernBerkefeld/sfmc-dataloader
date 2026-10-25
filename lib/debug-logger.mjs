import fs from 'node:fs';
import path from 'node:path';

/**
 * @typedef {object} DebugLogger
 * @property {string} logPath - Absolute path to the log file
 * @property {(text: string) => void} write - Append a line to the log file
 */

/**
 * Initialize a debug logger that writes API interactions to a timestamped log file.
 *
 * @param {string} projectRoot - mcdev project root directory
 * @param {string} version - mcdata version string
 * @param {string[]} argv - Full process.argv array
 * @returns {DebugLogger}
 */
export function initDebugLogger(projectRoot, version, argv) {
    const logsDirectory = path.join(projectRoot, 'logs', 'data');
    fs.mkdirSync(logsDirectory, { recursive: true });

    // Timestamp with dots instead of colons for Windows filesystem compatibility
    const ts = new Date().toISOString().replaceAll(':', '.');
    const logPath = path.join(logsDirectory, `${ts}.log`);

    // Redact only credential argument values in the diagnostic copy, never execution argv.
    let isSensitiveValue = false;
    const argumentsForLog = argv.slice(2).map((argument) => {
        if (isSensitiveValue) {
            isSensitiveValue = false;
            return '***';
        }
        const separator = argument.indexOf('=');
        const flag = separator === -1 ? argument : argument.slice(0, separator);
        if (flag === '--client-id' || flag === '--client-secret') {
            if (separator !== -1) {
                return `${flag}=***`;
            }
            isSensitiveValue = true;
        }
        return argument;
    });

    // Reconstruct command line for header, quoting args with spaces.
    const command =
        'mcdata ' +
        argumentsForLog
            .map((argument) => (argument.includes(' ') ? `"${argument}"` : argument))
            .join(' ');

    // Write header
    const header = `mcdata v${version}\nRan command: ${command}\n---\n`;
    fs.writeFileSync(logPath, header, 'utf8');

    return {
        logPath,
        write: (text) => fs.appendFileSync(logPath, text + '\n', 'utf8'),
    };
}
