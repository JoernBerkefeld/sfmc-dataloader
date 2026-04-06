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
