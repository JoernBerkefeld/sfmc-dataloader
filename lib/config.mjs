import fs from 'node:fs';
import path from 'node:path';
import { log } from './log.mjs';

export const FILE_MCDEV_RC = '.mcdevrc.json';
export const FILE_MCDEV_AUTH = '.mcdev-auth.json';
export const FILE_MCDATA_RC = '.mcdatarc.json';
export const FILE_MCDATA_AUTH = '.mcdata-auth.json';

export const WARN_MCDATA_SUPERSEDED =
    'mcdata: Using .mcdevrc.json / .mcdev-auth.json; .mcdatarc.json / .mcdata-auth.json are ignored.';

/**
 * @typedef {object} McdevrcCredentials
 * @property {number} [eid]
 * @property {Record<string, number|string>} businessUnits
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
 * @property {number} [account_id]
 */

/**
 * Loads project config from either mcdev or mcdata file pairs, applying mcdev-wins precedence.
 * If both mcdev files exist they are used and mcdata files (if also present) are ignored with a warning.
 * If only the mcdata pair exists it is used.
 * Partial pairs (one file without the other) or no files at all throw descriptive errors.
 *
 * @param {string} projectRoot
 * @param {{ stderr?: (msg: string) => void }} [options]
 * @returns {{ mcdevrc: Mcdevrc, mcdevAuth: Record<string, AuthCredential> }}
 */
export function loadProjectConfig(projectRoot, options = {}) {
    const err = options.stderr ?? ((msg) => log.error(msg));
    const rcMcdev = path.join(projectRoot, FILE_MCDEV_RC);
    const authMcdev = path.join(projectRoot, FILE_MCDEV_AUTH);
    const rcMcdata = path.join(projectRoot, FILE_MCDATA_RC);
    const authMcdata = path.join(projectRoot, FILE_MCDATA_AUTH);

    const hasMcdevRc = fs.existsSync(rcMcdev);
    const hasMcdevAuth = fs.existsSync(authMcdev);
    const hasMcdataRc = fs.existsSync(rcMcdata);
    const hasMcdataAuth = fs.existsSync(authMcdata);

    const mcdevPairComplete = hasMcdevRc && hasMcdevAuth;
    const mcdataPairComplete = hasMcdataRc && hasMcdataAuth;

    if (mcdevPairComplete) {
        if (hasMcdataRc || hasMcdataAuth) {
            err(WARN_MCDATA_SUPERSEDED);
        }
        const mcdevrc = JSON.parse(fs.readFileSync(rcMcdev, 'utf8'));
        const mcdevAuth = JSON.parse(fs.readFileSync(authMcdev, 'utf8'));
        return { mcdevrc, mcdevAuth };
    }

    if (hasMcdevRc !== hasMcdevAuth) {
        if (hasMcdevRc && !hasMcdevAuth) {
            throw new Error(`Missing ${authMcdev} (pair with ${FILE_MCDEV_RC})`);
        }
        throw new Error(`Missing ${rcMcdev} (pair with ${FILE_MCDEV_AUTH})`);
    }

    if (mcdataPairComplete) {
        const mcdevrc = JSON.parse(fs.readFileSync(rcMcdata, 'utf8'));
        const mcdevAuth = JSON.parse(fs.readFileSync(authMcdata, 'utf8'));
        return { mcdevrc, mcdevAuth };
    }

    if (hasMcdataRc !== hasMcdataAuth) {
        if (hasMcdataRc && !hasMcdataAuth) {
            throw new Error(`Missing ${authMcdata} (pair with ${FILE_MCDATA_RC})`);
        }
        throw new Error(`Missing ${rcMcdata} (pair with ${FILE_MCDATA_AUTH})`);
    }

    throw new Error(
        `No project config found in ${projectRoot}. Add ${FILE_MCDEV_RC} + ${FILE_MCDEV_AUTH} (mcdev), or ${FILE_MCDATA_RC} + ${FILE_MCDATA_AUTH} from \`mcdata init\`. Install mcdev globally (\`npm i -g mcdev\`) if you want a full mcdev project.`,
    );
}

/**
 * @param {string} projectRoot
 * @returns {{ mcdevrc: Mcdevrc, mcdevAuth: Record<string, AuthCredential> }}
 */
export function loadMcdevProject(projectRoot) {
    return loadProjectConfig(projectRoot);
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
        throw new Error(`Unknown credential "${credentialName}" in project credentials config`);
    }
    const midRaw = credBlock.businessUnits?.[buName];
    if (midRaw === undefined || midRaw === null) {
        throw new Error(`Unknown business unit "${buName}" under credential "${credentialName}"`);
    }
    const mid = typeof midRaw === 'number' ? midRaw : Number.parseInt(String(midRaw), 10);
    if (!Number.isInteger(mid)) {
        throw new TypeError(`Invalid MID for ${credentialName}/${buName}: ${midRaw}`);
    }
    const authCred = mcdevAuth[credentialName];
    if (!authCred?.client_id || !authCred?.client_secret || !authCred?.auth_url) {
        throw new Error(
            `Missing auth fields for credential "${credentialName}" in auth config file`,
        );
    }
    return { mid, authCred };
}

/**
 * Auth object for sfmc-sdk `Auth` / `SDK` constructor.
 *
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

/**
 * @typedef {object} DebugLogger
 * @property {string} logPath - Absolute path to the log file
 * @property {(text: string) => void} write - Append a line to the log file
 */

/**
 * Options object for sfmc-sdk `SDK` constructor.
 * When a logger is provided, includes event handlers to log API requests/responses to file.
 *
 * @param {DebugLogger|null} [logger] - Debug logger object, or null/undefined to disable logging
 * @returns {import('sfmc-sdk').SdkOptions}
 */
export function buildSdkOptions(logger = null) {
    /** @type {import('sfmc-sdk').SdkOptions} */
    const options = {
        requestAttempts: 3,
        retryOnConnectionError: true,
        eventHandlers: {
            onLoop: (_type, accumulator, context) => {
                if (context) {
                    process.stdout.write(
                        ` - Requesting batch ${context.nextPage} of ${context.totalPages} (${context.accumulatedCount} records so far)\n`,
                    );
                } else {
                    process.stdout.write(
                        ` - Requesting next batch (currently ${accumulator?.length ?? 0} records)\n`,
                    );
                }
            },
            onConnectionError: (ex, remainingAttempts) => {
                const endpointStr = ex.endpoint ? String(ex.endpoint) : '';
                const endpointSuffix = endpointStr
                    ? ` - ${endpointStr.split('rest.marketingcloudapis.com')[1] ?? endpointStr}`
                    : '';
                process.stdout.write(
                    ` - Connection problem (Code: ${ex.code}). Retrying ${remainingAttempts} time${
                        remainingAttempts > 1 ? 's' : ''
                    }${endpointSuffix}\n`,
                );
            },
        },
    };
    if (logger) {
        options.eventHandlers = {
            ...options.eventHandlers,
            logRequest: (req) => {
                const msg = structuredClone(req);
                if (msg.headers?.Authorization) {
                    msg.headers.Authorization = 'Bearer *** TOKEN REMOVED ***';
                }
                logger.write(`API REQUEST >> ${msg.method?.toUpperCase() || 'GET'} ${msg.url}`);
                if (msg.data) {
                    const body =
                        typeof msg.data === 'string' ? msg.data : JSON.stringify(msg.data, null, 2);
                    logger.write(`REQUEST BODY >> ${body}`);
                }
            },
            logResponse: (res) => {
                logger.write(`API RESPONSE << ${res.status || res.statusCode || '(no status)'}`);
                const body =
                    typeof res.data === 'string' ? res.data : JSON.stringify(res.data, null, 2);
                const indentedBody = body
                    .split('\n')
                    .map((line) => '  ' + line)
                    .join('\n');
                logger.write(`RESPONSE BODY <<\n${indentedBody}`);
            },
        };
    }
    return options;
}
