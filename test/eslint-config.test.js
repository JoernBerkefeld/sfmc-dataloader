import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { ESLint } from 'eslint';

const eslint = new ESLint({ fix: true });
const filePath = 'lib/jsdoc-regression.mjs';

describe('ESLint documentation configuration', () => {
    it('disables asterisk removal after the Unicorn recommended preset', async () => {
        const config = await eslint.calculateConfigForFile(filePath);
        assert.equal(config.rules['unicorn/no-asterisk-prefix-in-documentation-comments'][0], 0);
        assert.equal(config.rules['jsdoc/check-alignment'][0], 1);
        for (const rule of [
            'prefer-string-replace-all',
            'prefer-continue',
            'prefer-early-return',
            'prefer-ternary',
            'consistent-boolean-name',
            'no-unnecessary-parameters',
            'prefer-combined-guards',
            'prefer-boolean-return',
        ]) {
            assert.equal(config.rules[`unicorn/${rule}`][0], 2);
        }
    });

    it('preserves starred multiline JSDoc through autofix', async () => {
        const source = [
            '/**',
            ' * Return the supplied value.',
            ' *',
            ' * @param {string} value - The input value.',
            ' * @returns {string} The unchanged value.',
            ' */',
            'export function identity(value) {',
            '    return value;',
            '}',
            '',
        ].join('\n');
        const [result] = await eslint.lintText(source, { filePath });
        assert.equal(result.errorCount, 0);
        assert.equal(result.warningCount, 0);
        assert.equal(result.output ?? source, source);
    });
});
