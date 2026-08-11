/**
 * Timestamped logging for mcdata (local timezone via `Date`).
 * Operational messages use info / warn / error levels.
 * info goes to stdout; warn and error go to stderr.
 * When a debug logger is registered via setDebugLogger(), every log.* call
 * also appends the formatted line to the debug log file.
 */

/**
 * @type {{ current: import('./debug-logger.mjs').DebugLogger|null }}
 */
const debugLoggerReference = { current: null };

/**
 * @returns {string}
 */
export function formatTime() {
    return new Date().toLocaleTimeString([], {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
    });
}

/**
 * Register the debug file logger. Once set, every log.* call also appends to the log file.
 *
 * @param {import('./debug-logger.mjs').DebugLogger} logger
 */
export function setDebugLogger(logger) {
    debugLoggerReference.current = logger;
}

/**
 * @param {'info'|'warn'|'error'} level
 * @param {string} message
 */
function write(level, message) {
    const line = `${formatTime()} ${level}: ${message}`;
    debugLoggerReference.current?.write(line);
    if (level === 'error') {
        console.error(line);
    } else if (level === 'warn') {
        console.warn(line);
    } else {
        console.log(line);
    }
}

export const log = {
    /**
     * @param {string} message
     */
    info: (message) => write('info', message),
    /**
     * @param {string} message
     */
    warn: (message) => write('warn', message),
    /**
     * @param {string} message
     */
    error: (message) => write('error', message),
};
