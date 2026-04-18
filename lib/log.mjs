/**
 * Timestamped stderr logging for mcdata (local timezone via `Date`).
 * Operational messages use info / warn / error levels.
 */

function formatTime() {
    return new Date().toLocaleTimeString([], {
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
    });
}

/**
 * @param {'info'|'warn'|'error'} level
 * @param {string} message
 */
function write(level, message) {
    const line = `${formatTime()} ${level}: ${message}`;
    if (level === 'warn') {
        console.warn(line);
    } else {
        console.error(line);
    }
}

export const log = {
    /** @param {string} message */
    info: (message) => write('info', message),
    /** @param {string} message */
    warn: (message) => write('warn', message),
    /** @param {string} message */
    error: (message) => write('error', message),
};
