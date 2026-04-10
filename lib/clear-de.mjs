/**
 * Remove all rows from a Data Extension via SOAP Perform (ClearData).
 * Requires appropriate tenant and user rights; may fail for shared DEs from non-owner BUs.
 *
 * @param {*} soap - SDK soap instance
 * @param {string} customerKey - DE external key
 * @returns {Promise.<any>}
 */
export async function clearDataExtensionRows(soap, customerKey) {
    return soap.perform('DataExtension', 'ClearData', {
        CustomerKey: customerKey,
    });
}
