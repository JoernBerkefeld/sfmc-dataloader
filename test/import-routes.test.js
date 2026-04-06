import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { resolveImportRoute, asyncUpsertPath, syncUpsertPath, rowsetGetPath } from '../lib/import-routes.mjs';

describe('import routes', () => {
    it('async upsert path uses async dataextensions', () => {
        const p = asyncUpsertPath('MyKey');
        assert.ok(p.includes('/data/v1/async/dataextensions/'));
        assert.ok(p.includes('key:'));
    });

    it('sync upsert uses customobjectdata', () => {
        const p = syncUpsertPath('k');
        assert.ok(p.startsWith('/data/v1/customobjectdata/key/'));
    });

    it('rowset get path encodes key', () => {
        assert.ok(rowsetGetPath('a/b').includes(encodeURIComponent('a/b')));
    });

    it('resolveImportRoute rejects insert with async', () => {
        assert.throws(() => resolveImportRoute('async', 'insert'), /async/);
    });

    it('resolveImportRoute async + upsert is PUT', () => {
        const r = resolveImportRoute('async', 'upsert');
        assert.equal(r.method, 'PUT');
    });

    it('resolveImportRoute sync + insert is POST', () => {
        const r = resolveImportRoute('sync', 'insert');
        assert.equal(r.method, 'POST');
    });
});
