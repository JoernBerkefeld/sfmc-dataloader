import SDK from 'sfmc-sdk';
import { buildSdkOptions } from './config.mjs';

/**
 * Normalize a Business Unit name to a safe identifier, mirroring mcdev's convention:
 * strip non-word/non-space characters, replace spaces with underscores, collapse consecutive underscores.
 *
 * @param {string} name - Raw BU name from the API
 * @returns {string}
 */
export function normalizeBuName(name) {
    return name
        .replaceAll(/[^\w\s]/gi, '')
        .replaceAll(/ +/g, '_')
        .replaceAll(/__+/g, '_');
}

/**
 * @typedef {object} BusinessUnitsResult
 * @property {number} eid - Enterprise MID (ID of the parent BU)
 * @property {Record<string, number>} businessUnits - Normalized BU name → MID mapping; parent BU is stored under `_ParentBU_`
 */

/**
 * Process a SOAP BusinessUnit retrieve result into the mcdata config shape.
 * Exposed separately so it can be tested without a live SDK instance.
 *
 * @param {object[]} results - `buResult.Results` array from the SOAP API
 * @param {number} enterpriseId - Fallback EID when no parent row is found
 * @returns {BusinessUnitsResult}
 */
export function processBusinessUnitResults(results, enterpriseId) {
    /** @type {Record<string, number>} */
    const businessUnits = {};
    let eid = enterpriseId;

    for (const row of results) {
        const id = Number.parseInt(row.ID, 10);
        const parentId = Number.parseInt(row.ParentID, 10);

        if (parentId === 0) {
            businessUnits['_ParentBU_'] = id;
            eid = id;
        } else {
            businessUnits[normalizeBuName(row.Name)] = id;
        }
    }

    /** @type {Record<string, number>} */
    const sorted = {};
    if (Object.hasOwn(businessUnits, '_ParentBU_')) {
        sorted['_ParentBU_'] = businessUnits['_ParentBU_'];
    }
    for (const key of Object.keys(businessUnits)
        .filter((k) => k !== '_ParentBU_')
        .toSorted((a, b) => a.localeCompare(b, undefined, { sensitivity: 'base' }))) {
        sorted[key] = businessUnits[key];
    }

    return { eid, businessUnits: sorted };
}

/**
 * Fetch all Business Units for a Marketing Cloud instance via the SOAP API.
 * Instantiates a temporary SDK client scoped to the enterprise account (`account_id: enterpriseId`)
 * and calls `BusinessUnit` retrieve with `QueryAllAccounts: true`.
 *
 * @param {{ client_id: string, client_secret: string, auth_url: string }} authCred
 * @param {number} enterpriseId - Enterprise MID (parent account ID)
 * @returns {Promise.<BusinessUnitsResult>}
 */
export async function fetchBusinessUnits(authCred, enterpriseId) {
    const sdk = new SDK(
        {
            client_id: authCred.client_id,
            client_secret: authCred.client_secret,
            auth_url: authCred.auth_url,
            account_id: enterpriseId,
        },
        buildSdkOptions(),
    );

    let buResult;
    try {
        buResult = await sdk.soap.retrieve(
            'BusinessUnit',
            ['Name', 'ID', 'ParentName', 'ParentID', 'IsActive'],
            { QueryAllAccounts: true },
        );
    } catch (ex) {
        throw new Error(
            `Could not retrieve Business Units - check client_id, client_secret, auth_url, and enterprise MID.`,
            { cause: ex },
        );
    }

    if (!buResult?.Results?.length) {
        throw new Error(
            'Credentials accepted but no Business Units returned. Verify this installed package has access to the parent BU.',
        );
    }

    return processBusinessUnitResults(buResult.Results, enterpriseId);
}
