import fs from 'node:fs';
import path from 'node:path';

/**
 * @typedef {object} McdevrcCredentials
 * @property {Record<string, Record<string, number|string>>} businessUnits
 */

/**
 * @typedef {object} Mcdevrc
 * @property {Record<string, McdevrcCredentials>} credentials
 */

/**
 * @typedef {object} AuthCredential
 * @property {string} client_id
 * @property {string} client_secret
 * @property {string} auth_url
 */

/**
 * @param {string} projectRoot
 * @returns {{ mcdevrc: Mcdevrc, mcdevAuth: Record<string, AuthCredential> }}
 */
export function loadMcdevProject(projectRoot) {
    const rcPath = path.join(projectRoot, '.mcdevrc.json');
    const authPath = path.join(projectRoot, '.mcdev-auth.json');
    if (!fs.existsSync(rcPath)) {
        throw new Error(`Missing ${rcPath}`);
    }
    if (!fs.existsSync(authPath)) {
        throw new Error(`Missing ${authPath}`);
    }
    const mcdevrc = JSON.parse(fs.readFileSync(rcPath, 'utf8'));
    const mcdevAuth = JSON.parse(fs.readFileSync(authPath, 'utf8'));
    return { mcdevrc, mcdevAuth };
}

/**
 * @param {string} credBu - `CredentialName/BUName`
 * @returns {{ credential: string, bu: string }}
 */
export function parseCredBu(credBu) {
    const slash = credBu.indexOf('/');
    if (slash <= 0 || slash === credBu.length - 1) {
        throw new Error(`Expected <credential>/<businessUnit>, got: ${credBu}`);
    }
    return {
        credential: credBu.slice(0, slash),
        bu: credBu.slice(slash + 1),
    };
}

/**
 * @param {Mcdevrc} mcdevrc
 * @param {Record<string, AuthCredential>} mcdevAuth
 * @param {string} credentialName
 * @param {string} buName
 * @returns {{ mid: number, authCred: AuthCredential }}
 */
export function resolveCredentialAndMid(mcdevrc, mcdevAuth, credentialName, buName) {
    const credBlock = mcdevrc.credentials?.[credentialName];
    if (!credBlock) {
        throw new Error(`Unknown credential "${credentialName}" in .mcdevrc.json`);
    }
    const midRaw = credBlock.businessUnits?.[buName];
    if (midRaw === undefined || midRaw === null) {
        throw new Error(`Unknown business unit "${buName}" under credential "${credentialName}"`);
    }
    const mid =
        typeof midRaw === 'number' ? midRaw : Number.parseInt(String(midRaw), 10);
    if (!Number.isInteger(mid)) {
        throw new Error(`Invalid MID for ${credentialName}/${buName}: ${midRaw}`);
    }
    const authCred = mcdevAuth[credentialName];
    if (!authCred?.client_id || !authCred?.client_secret || !authCred?.auth_url) {
        throw new Error(`Missing auth fields for credential "${credentialName}" in .mcdev-auth.json`);
    }
    return { mid, authCred };
}

/**
 * Auth object for sfmc-sdk `Auth` / `SDK` constructor.
 * @param {AuthCredential} authCred
 * @param {number} mid
 * @returns {import('sfmc-sdk').AuthObject}
 */
export function buildSdkAuthObject(authCred, mid) {
    return {
        client_id: authCred.client_id,
        client_secret: authCred.client_secret,
        auth_url: authCred.auth_url,
        account_id: mid,
    };
}
