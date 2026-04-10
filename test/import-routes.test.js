import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import {
    resolveImportRoute,
    asyncDataExtensionRowsPath,
    rowsetGetPath,
} from '../lib/import-routes.mjs';

describe('import routes', () => {
    it('async rows path uses async dataextensions', () => {
        const p = asyncDataExtensionRowsPath('MyKey');
        assert.ok(p.includes('/data/v1/async/dataextensions/'));
        assert.ok(p.includes('key:'));
    });

    it('rowset get path encodes key', () => {
        assert.ok(rowsetGetPath('a/b').includes(encodeURIComponent('a/b')));
    });

    it('resolveImportRoute upsert is PUT', () => {
        const r = resolveImportRoute('upsert');
        assert.equal(r.method, 'PUT');
        assert.equal(r.path('k'), asyncDataExtensionRowsPath('k'));
    });

    it('resolveImportRoute insert is POST', () => {
        const r = resolveImportRoute('insert');
        assert.equal(r.method, 'POST');
    });

    it('resolveImportRoute rejects unknown mode', () => {
        assert.throws(() => resolveImportRoute('update'), /Unsupported/);
    });
});
