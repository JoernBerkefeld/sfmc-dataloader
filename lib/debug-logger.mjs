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
    const logsDir = path.join(projectRoot, 'logs', 'data');
    fs.mkdirSync(logsDir, { recursive: true });

    // Timestamp with dots instead of colons for Windows filesystem compatibility
    const ts = new Date().toISOString().replaceAll(':', '.');
    const logPath = path.join(logsDir, `${ts}.log`);

    // Reconstruct command line for header, quoting args with spaces
    const command =
        'mcdata ' +
        argv
            .slice(2)
            .map((arg) => (arg.includes(' ') ? `"${arg}"` : arg))
            .join(' ');

    // Write header
    const header = `mcdata v${version}\nRan command: ${command}\n---\n`;
    fs.writeFileSync(logPath, header, 'utf8');

    return {
        logPath,
        write: (text) => fs.appendFileSync(logPath, text + '\n', 'utf8'),
    };
}
