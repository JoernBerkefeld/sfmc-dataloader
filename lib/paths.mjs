import path from 'node:path';

/**
 * @param {string} projectRoot
 * @param {string} credentialName
 * @param {string} buName
 * @returns {string} absolute path ./data/<cred>/<bu>/
 */
export function dataDirectoryForBu(projectRoot, credentialName, buName) {
    return path.join(projectRoot, 'data', credentialName, buName);
}

/**
 * Path relative to mcdev project root for logs (POSIX-style, `./`-prefixed when needed).
 *
 * @param {string} projectRoot
 * @param {string} absolutePath
 * @returns {string}
 */
export function projectRelativePosix(projectRoot, absolutePath) {
    const rel = path.relative(path.resolve(projectRoot), path.resolve(absolutePath));
    const norm = rel.split(path.sep).join('/');
    if (norm === '' || norm === '.') {
        return './';
    }
    return norm.startsWith('.') ? norm : `./${norm}`;
}
