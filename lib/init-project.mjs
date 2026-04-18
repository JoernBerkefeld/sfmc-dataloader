import fs from 'node:fs';
import path from 'node:path';
import readline from 'node:readline/promises';
import { stdin as input, stdout as output } from 'node:process';
import { FILE_MCDEV_RC, FILE_MCDEV_AUTH, FILE_MCDATA_RC, FILE_MCDATA_AUTH } from './config.mjs';
import { fetchBusinessUnits } from './business-units.mjs';
import { log } from './log.mjs';

/**
 * @typedef {object} InitOptions
 * @property {string} projectRoot - Absolute path to the project folder
 * @property {boolean} isTTY - Whether stdin is a TTY (interactive)
 * @property {string} [credential] - Non-interactive: credential name
 * @property {string} [clientId] - Non-interactive: client_id
 * @property {string} [clientSecret] - Non-interactive: client_secret
 * @property {string} [authUrl] - Non-interactive: auth_url
 * @property {string} [enterpriseId] - Non-interactive: enterprise MID (string, will be parsed to int)
 * @property {boolean} [yes] - Skip overwrite confirmation
 * @property {Function} [_buFetcher] - Dependency injection for testing (replaces fetchBusinessUnits)
 * @property {(question: string) => Promise.<boolean>} [_confirm] - Dependency injection for testing (replaces defaultConfirm)
 * @property {(msg: string) => void} [stdout] - Override stdout (for testing)
 * @property {(msg: string) => void} [stderr] - Override stderr (for testing)
 */

/**
 * Ask a yes/no question on the terminal and return true only if the user types "y" or "yes".
 *
 * @param {string} question
 * @returns {Promise.<boolean>}
 */
async function defaultConfirm(question) {
    const rl = readline.createInterface({ input, output });
    try {
        const answer = (await rl.question(question)).trim().toLowerCase();
        return answer === 'y' || answer === 'yes';
    } finally {
        rl.close();
    }
}

/**
 * Ensure .mcdata-auth.json is listed in the project's .gitignore (creates the file if absent).
 *
 * @param {string} projectRoot
 */
function ensureGitignore(projectRoot) {
    const gitignorePath = path.join(projectRoot, '.gitignore');
    const entry = '.mcdata-auth.json';
    if (!fs.existsSync(gitignorePath)) {
        fs.writeFileSync(gitignorePath, `${entry}\n`, 'utf8');
        return;
    }
    const current = fs.readFileSync(gitignorePath, 'utf8');
    const lines = current.split('\n');
    if (!lines.some((line) => line.trim() === entry)) {
        const appended = current.endsWith('\n')
            ? current + entry + '\n'
            : current + '\n' + entry + '\n';
        fs.writeFileSync(gitignorePath, appended, 'utf8');
    }
}

/**
 * Run the `mcdata init` flow — interactive or non-interactive.
 *
 * @param {InitOptions} opts
 * @returns {Promise.<number>} exit code (0 = success, 1 = failure)
 */
export async function runMcdataInit(opts) {
    const { projectRoot, isTTY, yes = false, _buFetcher } = opts;
    const out = opts.stdout ?? ((msg) => console.log(msg));
    const err = opts.stderr ?? ((msg) => log.error(msg));

    // Guard: do not init over an existing mcdev project (both files must be present)
    if (
        fs.existsSync(path.join(projectRoot, FILE_MCDEV_RC)) &&
        fs.existsSync(path.join(projectRoot, FILE_MCDEV_AUTH))
    ) {
        err(
            `This project is managed by mcdev (${FILE_MCDEV_RC} and ${FILE_MCDEV_AUTH} found).\n` +
                `Manage your credentials by editing ${FILE_MCDEV_AUTH} directly, or run 'mcdev init' to re-initialise.`,
        );
        return 1;
    }

    // Guard: confirm overwrite when mcdata files already present
    const mcdataRcPath = path.join(projectRoot, FILE_MCDATA_RC);
    const mcdataAuthPath = path.join(projectRoot, FILE_MCDATA_AUTH);
    if (!yes && (fs.existsSync(mcdataRcPath) || fs.existsSync(mcdataAuthPath))) {
        if (isTTY) {
            const confirmFn = opts._confirm ?? defaultConfirm;
            const confirmed = await confirmFn(
                `${FILE_MCDATA_RC} or ${FILE_MCDATA_AUTH} already exists. Override existing configuration? [y/N] `,
            );
            if (!confirmed) {
                out('Aborted.');
                return 1;
            }
        } else {
            err(
                `${FILE_MCDATA_RC} or ${FILE_MCDATA_AUTH} already exists in ${projectRoot}.\n` +
                    `Pass --yes to overwrite.`,
            );
            return 1;
        }
    }

    // Collect credentials
    let credentialName = opts.credential;
    let clientId = opts.clientId;
    let clientSecret = opts.clientSecret;
    let authUrl = opts.authUrl;
    let enterpriseIdStr = opts.enterpriseId;

    if (isTTY && (!credentialName || !clientId || !clientSecret || !authUrl || !enterpriseIdStr)) {
        const rl = readline.createInterface({ input, output });
        try {
            if (!credentialName) {
                credentialName = (await rl.question('Credential name (e.g. MyOrg): ')).trim();
            }
            if (!clientId) {
                clientId = (await rl.question('Client ID: ')).trim();
            }
            if (!clientSecret) {
                clientSecret = (await rl.question('Client Secret: ')).trim();
            }
            if (!authUrl) {
                authUrl = (
                    await rl.question(
                        'Auth URL (e.g. https://<tenantsubdomain>.auth.marketingcloudapis.com/): ',
                    )
                ).trim();
            }
            if (!enterpriseIdStr) {
                enterpriseIdStr = (await rl.question('Enterprise MID: ')).trim();
            }
        } finally {
            rl.close();
        }
    } else if (!isTTY) {
        // Non-interactive: all flags must be provided
        const missing = [];
        if (!credentialName) {
            missing.push('--credential');
        }
        if (!clientId) {
            missing.push('--client-id');
        }
        if (!clientSecret) {
            missing.push('--client-secret');
        }
        if (!authUrl) {
            missing.push('--auth-url');
        }
        if (!enterpriseIdStr) {
            missing.push('--enterprise-id');
        }
        if (missing.length > 0) {
            err(
                `mcdata init: missing required flags in non-interactive mode: ${missing.join(', ')}`,
            );
            return 1;
        }
    }

    const enterpriseId = Number.parseInt(String(enterpriseIdStr), 10);
    if (!Number.isInteger(enterpriseId) || enterpriseId <= 0) {
        err(`Invalid enterprise MID: ${enterpriseIdStr}`);
        return 1;
    }

    // Fetch Business Units
    out('Fetching Business Units from Marketing Cloud...');
    let buResult;
    try {
        const buFetcher = _buFetcher ?? fetchBusinessUnits;
        buResult = await buFetcher(
            { client_id: clientId, client_secret: clientSecret, auth_url: authUrl },
            enterpriseId,
        );
    } catch (ex) {
        err(`Failed to fetch Business Units: ${ex.message}`);
        return 1;
    }
    out(`Found ${Object.keys(buResult.businessUnits).length} Business Unit(s).`);

    // Build config objects
    const mcdataRc = {
        credentials: {
            [credentialName]: {
                eid: buResult.eid,
                businessUnits: buResult.businessUnits,
            },
        },
    };

    const mcdataAuth = {
        [credentialName]: {
            client_id: clientId,
            client_secret: clientSecret,
            auth_url: authUrl,
            account_id: buResult.eid,
        },
    };

    // Write files
    fs.writeFileSync(mcdataRcPath, JSON.stringify(mcdataRc, null, 4) + '\n', 'utf8');
    fs.writeFileSync(mcdataAuthPath, JSON.stringify(mcdataAuth, null, 4) + '\n', 'utf8');
    ensureGitignore(projectRoot);

    out(`Created ${FILE_MCDATA_RC} and ${FILE_MCDATA_AUTH} in ${projectRoot}`);
    out('Make sure to add .mcdata-auth.json to your .gitignore (done automatically).');
    out("Tip: To use mcdev instead, install it globally and run 'mcdev init'.");
    return 0;
}
