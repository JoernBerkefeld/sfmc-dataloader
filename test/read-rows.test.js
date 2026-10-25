import assert from 'node:assert/strict';
import { describe, it, before } from 'node:test';
import { Readable } from 'node:stream';
import csv from '@joern.berkefeld/csv-parser';
import fs from 'node:fs/promises';
import { createFixtureRoot } from './fixture-root.js';
import path from 'node:path';
import {
    countDataRowsFromImportPaths,
    preflightImportPaths,
    readRowsFromFile,
    readRowsFromImportPaths,
    streamRowsFromFile,
    streamRowsFromImportPaths,
} from '../lib/read-rows.mjs';

/**
 * @param {AsyncIterable<object>} gen
 * @returns {Promise.<object[]>}
 */
async function collectAsync(gen) {
    const out = await Array.fromAsync(gen);
    return out;
}

const encode = (value) => `"${value.replaceAll('"', '""')}"`;

describe('strict fork regressions', () => {
    let temporary;
    before(async () => {
        temporary = await createFixtureRoot('mcdata-strict-');
    });
    for (const format of ['csv', 'tsv']) {
        const separator = format === 'tsv' ? '\t' : ',';
        it(`${format}: preserves the original 87-column inline-quote failure at chunk boundaries`, async () => {
            const headers = Array.from({ length: 87 }, (_, index) => `field${index}`);
            const values = headers.map((_, index) =>
                index === 9 ? 'var label = "demo";' : `value-${index}`,
            );
            const next = headers.map((_, index) => `next-${index}`);
            const text = [headers, values, next].map((row) => row.join(separator)).join('\n');
            const expected = [values, next].map((row) =>
                Object.fromEntries(headers.map((header, index) => [header, row[index]])),
            );
            const bytes = Buffer.from(text);
            for (const size of [bytes.length, 1, 7, 64]) {
                const chunks = [];
                for (let offset = 0; offset < bytes.length; offset += size) {
                    chunks.push(Buffer.from(bytes.subarray(offset, offset + size)));
                }
                const parser = csv({ separator, strict: true, strictQuotes: true });
                const parsed = await collectAsync(Readable.from(chunks).pipe(parser));
                assert.deepEqual(parsed, expected);
            }
            const file = path.join(temporary, `incident.${format}`);
            await fs.writeFile(file, text);
            assert.deepEqual(await readRowsFromFile(file, format), expected);
            assert.deepEqual(await collectAsync(streamRowsFromFile(file, format)), expected);
            const continuation = path.join(temporary, `continuation.${format}`);
            await fs.writeFile(continuation, next.join(separator));
            const metadata = await preflightImportPaths([file, continuation], format);
            assert.deepEqual(metadata.headers, headers);
            assert.equal(metadata.recordCount, 3);
            assert.deepEqual(
                await readRowsFromImportPaths([file, continuation], format, metadata),
                [...expected, expected[1]],
            );
        });
        it(`${format}: preserves literal surrounding quotes, multiline Unicode and empty cells`, async () => {
            const headers = ['"literal"', 'content', 'empty'];
            const values = ['"wrapped"', `Δ${separator}line\nnext`, ''];
            const text = [headers, values]
                .map((row) => row.map(encode).join(separator))
                .join('\r\n');
            const file = path.join(temporary, `literal.${format}`);
            await fs.writeFile(file, text);
            assert.deepEqual(await readRowsFromFile(file, format), [
                Object.fromEntries(headers.map((header, index) => [header, values[index]])),
            ]);
        });
        it(`${format}: parses BOM before complex quoted headers with exact validation/read parity`, async () => {
            const headers = [`"Δ${separator}first\nline"`, 'content', 'empty'];
            const values = ['"wrapped"', `001${separator}next\n"quoted"`, ''];
            const next = ['false', '\u{FEFF}embedded', '0007'];
            const expected = [values, next].map((row) =>
                Object.fromEntries(headers.map((header, index) => [header, row[index]])),
            );
            const headerText = headers.map(encode).join(separator);
            const dataText = values.map(encode).join(separator);
            const file = path.join(temporary, `complex-bom.${format}`);
            const continuation = path.join(temporary, `complex-bom-continuation.${format}`);
            await fs.writeFile(file, `\u{FEFF}${headerText}\r\n${dataText}\r\n`);
            await fs.writeFile(continuation, `\u{FEFF}${next.map(encode).join(separator)}\r\n`);
            const paths = [file, continuation];
            const metadata = await preflightImportPaths(paths, format);
            assert.deepEqual(metadata.headers, headers);
            assert.equal(metadata.recordCount, expected.length);
            assert.equal(await countDataRowsFromImportPaths(paths, format), expected.length);
            assert.deepEqual(await readRowsFromFile(file, format), [expected[0]]);
            assert.deepEqual(await collectAsync(streamRowsFromFile(file, format)), [expected[0]]);
            assert.deepEqual(
                await readRowsFromFile(continuation, format, { columnHeaders: headers }),
                [expected[1]],
            );
            assert.deepEqual(await readRowsFromImportPaths(paths, format, metadata), expected);
            assert.deepEqual(
                await collectAsync(streamRowsFromImportPaths(paths, format, metadata)),
                expected,
            );
            // Header-only first parts must establish the same literal column names.
            await fs.writeFile(file, `\u{FEFF}${headerText}\r\n`);
            const headerOnly = await preflightImportPaths(paths, format);
            assert.deepEqual(headerOnly.headers, headers);
            assert.equal(headerOnly.recordCount, 1);
            assert.deepEqual(await readRowsFromImportPaths(paths, format, headerOnly), [
                expected[1],
            ]);
        });
        it(`${format}: preserves explicit headers and strips only the leading BOM in continuation values`, async () => {
            const headers = ['\u{FEFF}"literal"', 'content'];
            const values = ['"wrapped"', '\u{FEFF}literal'];
            const expected = [
                Object.fromEntries(headers.map((header, index) => [header, values[index]])),
            ];
            const file = path.join(temporary, `explicit-bom.${format}`);
            await fs.writeFile(file, `\u{FEFF}${values.map(encode).join(separator)}\n`);
            const options = { columnHeaders: headers };
            assert.deepEqual(await readRowsFromFile(file, format, options), expected);
            assert.deepEqual(
                await collectAsync(streamRowsFromFile(file, format, options)),
                expected,
            );
        });
        it(`${format}: uses header-only first parts and reuses validated counts`, async () => {
            const first = path.join(temporary, `header.${format}`);
            const second = path.join(temporary, `data.${format}`);
            await fs.writeFile(first, `a${separator}b\n`);
            await fs.writeFile(second, `1${separator}2\n`);
            const paths = [first, second];
            const metadata = await preflightImportPaths(paths, format);
            assert.deepEqual(metadata, {
                paths,
                format,
                headers: ['a', 'b'],
                recordCount: 1,
                skipEmptyLines: false,
            });
            assert.ok(
                Object.isFrozen(metadata) &&
                    Object.isFrozen(metadata.headers) &&
                    Object.isFrozen(metadata.paths),
            );
            assert.deepEqual(
                await collectAsync(streamRowsFromImportPaths(paths, format, metadata)),
                [{ a: '1', b: '2' }],
            );
            await fs.unlink(second);
            assert.equal(await countDataRowsFromImportPaths(paths, format, metadata), 1);
        });
    }
    it('rejects late width/quote failures and duplicate effective headers before consumption', async () => {
        for (const [index, text] of [
            'a,b\n1,2\n3\n',
            'a,b\n1,2\n"open,4',
            'a,b\n"closed"tail,2',
            'a,a\n1,2',
            '\u{FEFF}a,a\n1,2',
        ].entries()) {
            const file = path.join(temporary, `bad-${index}.csv`);
            await fs.writeFile(file, text);
            await assert.rejects(preflightImportPaths([file], 'csv'));
            await assert.rejects(readRowsFromFile(file, 'csv'));
            await assert.rejects(collectAsync(streamRowsFromFile(file, 'csv')));
        }
        await assert.rejects(
            readRowsFromFile(path.join(temporary, 'missing.csv'), 'csv'),
            /missing.csv/,
        );
    });
    it('reports no data rows for empty delimited files and header-only sets', async () => {
        for (const format of ['csv', 'tsv']) {
            const file = path.join(temporary, `empty.${format}`);
            for (const text of ['', '\u{FEFF}']) {
                await fs.writeFile(file, text);
                await assert.rejects(preflightImportPaths([file], format), (ex) => {
                    assert.match(ex.message, /no data rows/);
                    assert.match(ex.message, /no column headers/);
                    assert.ok(ex.message.includes(file));
                    return true;
                });
                await assert.rejects(countDataRowsFromImportPaths([file], format), /no data rows/);
                assert.deepEqual(await readRowsFromFile(file, format), []);
                assert.deepEqual(await collectAsync(streamRowsFromFile(file, format)), []);
            }
            await fs.writeFile(file, format === 'csv' ? 'a,b\n' : 'a\tb\n');
            await assert.rejects(preflightImportPaths([file], format), /no data rows/);
        }
    });
    it('validates JSON syntax/shape and whole-set emptiness', async () => {
        const file = path.join(temporary, 'invalid.json');
        for (const text of ['{', '{}', '[]', '{"items":[]}']) {
            await fs.writeFile(file, text);
            await assert.rejects(preflightImportPaths([file], 'json'));
        }
        await fs.writeFile(file, '{"items":[{"id":"one"}]}');
        assert.equal((await preflightImportPaths([file], 'json')).recordCount, 1);
    });
    it('closes owned streams when consumption stops early', async () => {
        const file = path.join(temporary, 'early.csv');
        await fs.writeFile(file, 'a,b\n' + '1,2\n'.repeat(1000));
        const rows = streamRowsFromFile(file, 'csv');
        assert.deepEqual((await rows.next()).value, { a: '1', b: '2' });
        await rows.return();
        await fs.rename(file, file + '.closed');
    });
});

describe('opt-in empty-line reader options', () => {
    let temporary;
    before(async () => {
        temporary = await createFixtureRoot('mcdata-empty-lines-');
    });
    for (const format of ['csv', 'tsv']) {
        const separator = format === 'tsv' ? '\t' : ',';
        it(`${format}: preserves exact valid values with omitted/false/true options`, async () => {
            const file = path.join(temporary, `valid.${format}`);
            const headers = ['id', 'content', 'empty'];
            const values = [
                ['001', `var label = "demo";${separator}Δ`, ''],
                ['', 'line\n\n  \nnext', ''],
                ['', '', ''],
            ];
            await fs.writeFile(
                file,
                '\u{FEFF}' +
                    [headers, ...values].map((row) => row.map(encode).join(separator)).join('\r\n'),
            );
            const expected = values.map((row) =>
                Object.fromEntries(headers.map((header, index) => [header, row[index]])),
            );
            for (const options of [{}, { skipEmptyLines: false }, { skipEmptyLines: true }]) {
                const metadata = await preflightImportPaths([file], format, options);
                assert.equal(metadata.recordCount, expected.length);
                assert.equal(metadata.skipEmptyLines, options.skipEmptyLines === true);
                assert.deepEqual(metadata.headers, headers);
                assert.deepEqual(await readRowsFromFile(file, format, options), expected);
                assert.deepEqual(
                    await collectAsync(streamRowsFromFile(file, format, options)),
                    expected,
                );
                assert.deepEqual(
                    await readRowsFromImportPaths([file], format, undefined, options),
                    expected,
                );
                assert.equal(
                    await countDataRowsFromImportPaths([file], format, undefined, options),
                    expected.length,
                );
            }
        });
        it(`${format}: skips only unquoted blank records and freezes multipart policy`, async () => {
            const first = path.join(temporary, `blank.${format}`);
            const second = path.join(temporary, `blank-continuation.${format}`);
            await fs.writeFile(
                first,
                '\u{FEFF}value\r\n\r\n   \r\n""\r\n"   "\r\n"line\n\n  \nnext"\r\n',
            );
            await fs.writeFile(second, '\u{FEFF}first continuation\n\n \nlast');
            const paths = [first, second];
            const options = { skipEmptyLines: true };
            const metadata = await preflightImportPaths(paths, format, options);
            options.skipEmptyLines = false;
            assert.ok(Object.isFrozen(metadata));
            assert.equal(metadata.skipEmptyLines, true);
            const expected = [
                { value: '' },
                { value: ' '.repeat(3) },
                { value: 'line\n\n  \nnext' },
                { value: 'first continuation' },
                { value: 'last' },
            ];
            assert.deepEqual(metadata.headers, ['value']);
            assert.equal(metadata.recordCount, expected.length);
            assert.deepEqual(
                await readRowsFromImportPaths([], 'json', metadata, options),
                expected,
            );
            assert.deepEqual(
                await collectAsync(
                    streamRowsFromImportPaths(paths, format, undefined, { skipEmptyLines: true }),
                ),
                expected,
            );
            assert.equal(
                await countDataRowsFromImportPaths(paths, format, metadata, options),
                expected.length,
            );
            assert.deepEqual(
                await readRowsFromFile(second, format, {
                    columnHeaders: ['value'],
                    skipEmptyLines: true,
                }),
                expected.slice(3),
            );
            // Default strict parsing still rejects zero-cell blank records.
            for (const defaults of [{}, { skipEmptyLines: false }]) {
                await assert.rejects(preflightImportPaths(paths, format, defaults));
                await assert.rejects(readRowsFromImportPaths(paths, format, undefined, defaults));
                await assert.rejects(readRowsFromFile(first, format, defaults));
            }
            await fs.writeFile(first, 'value\n   \nretained');
            const legacy = await preflightImportPaths([first], format);
            assert.deepEqual(
                await readRowsFromImportPaths([first], format, legacy, { skipEmptyLines: true }),
                [{ value: ' '.repeat(3) }, { value: 'retained' }],
            );
        });
        it(`${format}: preserves delimiter-only width checks and terminal late errors`, async () => {
            const file = path.join(temporary, `width.${format}`);
            const options = { skipEmptyLines: true };
            await fs.writeFile(file, `a${separator}b\n\n \n${separator}\n`);
            assert.deepEqual(await readRowsFromFile(file, format, options), [{ a: '', b: '' }]);
            assert.equal((await preflightImportPaths([file], format, options)).recordCount, 1);
            for (const text of [
                `value\n${separator}\n`,
                `a${separator}b\n${`1${separator}2\n`.repeat(100)}\n \nlate\n`,
            ]) {
                await fs.writeFile(file, text);
                await assert.rejects(preflightImportPaths([file], format, options));
                await assert.rejects(readRowsFromFile(file, format, options));
                await assert.rejects(collectAsync(streamRowsFromFile(file, format, options)));
            }
        });
        it(`${format}: awaits slow consumers and closes on early return`, async () => {
            const file = path.join(temporary, `slow.${format}`);
            await fs.writeFile(file, 'value\n' + 'row\n\n \n'.repeat(1000));
            const metadata = await preflightImportPaths([file], format, { skipEmptyLines: true });
            let count = 0;
            for await (const row of streamRowsFromImportPaths([file], format, metadata)) {
                assert.deepEqual(row, { value: 'row' });
                await new Promise((resolve) => setImmediate(resolve));
                count++;
            }
            assert.equal(count, metadata.recordCount);
            const rows = streamRowsFromImportPaths([file], format, metadata);
            assert.deepEqual((await rows.next()).value, { value: 'row' });
            await rows.return();
            await fs.rename(file, file + '.closed');
        });
    }
    it('leaves JSON rows and counts unchanged', async () => {
        const file = path.join(temporary, 'unchanged.json');
        const expected = [{ value: '' }, { value: '  ' }];
        await fs.writeFile(file, JSON.stringify({ items: expected }));
        const options = { skipEmptyLines: true };
        const metadata = await preflightImportPaths([file], 'json', options);
        assert.equal(metadata.skipEmptyLines, false);
        assert.equal(metadata.recordCount, 2);
        assert.deepEqual(await readRowsFromFile(file, 'json', options), expected);
        assert.deepEqual(await collectAsync(streamRowsFromFile(file, 'json', options)), expected);
        assert.deepEqual(
            await readRowsFromImportPaths([file], 'json', undefined, options),
            expected,
        );
    });
});

describe('readRowsFromFile', () => {
    let temporary;
    before(async () => {
        temporary = await createFixtureRoot('mcdata-read-');
    });
    // Keep run-owned fixtures for diagnostics; never recursively delete the run root.

    it('parses JSON array', async () => {
        const p = path.join(temporary, 'a.json');
        await fs.writeFile(p, JSON.stringify([{ a: 1 }]), 'utf8');
        const rows = await readRowsFromFile(p, 'json');
        assert.deepEqual(rows, [{ a: 1 }]);
    });

    it('parses JSON items wrapper', async () => {
        const p = path.join(temporary, 'b.json');
        await fs.writeFile(p, JSON.stringify({ items: [{ b: 2 }] }), 'utf8');
        const rows = await readRowsFromFile(p, 'json');
        assert.deepEqual(rows, [{ b: 2 }]);
    });

    it('reads CSV rows', async () => {
        const p = path.join(temporary, 'c.csv');
        await fs.writeFile(p, 'col1,col2\n1,2\n', 'utf8');
        const rows = await readRowsFromFile(p, 'csv');
        assert.deepEqual(rows, [{ col1: '1', col2: '2' }]);
    });

    it('reads TSV rows', async () => {
        const p = path.join(temporary, 'd.tsv');
        await fs.writeFile(p, 'col1\tcol2\n1\t2\n', 'utf8');
        const rows = await readRowsFromFile(p, 'tsv');
        assert.deepEqual(rows, [{ col1: '1', col2: '2' }]);
    });

    it('strips BOM from first header field in TSV', async () => {
        const p = path.join(temporary, 'bom.tsv');
        const bom = '\u{FEFF}';
        await fs.writeFile(p, `${bom}col1\tcol2\n1\t2\n`, 'utf8');
        const rows = await readRowsFromFile(p, 'tsv');
        assert.deepEqual(rows, [{ col1: '1', col2: '2' }]);
        assert.ok(
            Object.keys(rows[0]).every((k) => !k.includes('\u{FEFF}')),
            'no BOM in keys',
        );
    });

    it('strips surrounding quotes from TSV headers and values', async () => {
        const p = path.join(temporary, 'quoted.tsv');
        await fs.writeFile(p, '"col1"\t"col2"\n"val1"\t"val2"\n', 'utf8');
        const rows = await readRowsFromFile(p, 'tsv');
        assert.deepEqual(rows, [{ col1: 'val1', col2: 'val2' }]);
    });

    it('handles BOM + quoted headers in TSV', async () => {
        const p = path.join(temporary, 'bom-quoted.tsv');
        const bom = '\u{FEFF}';
        await fs.writeFile(p, `${bom}"col1"\t"col2"\n"v1"\t"v2"\n`, 'utf8');
        const rows = await readRowsFromFile(p, 'tsv');
        assert.deepEqual(rows, [{ col1: 'v1', col2: 'v2' }]);
        assert.ok(
            Object.keys(rows[0]).every((k) => !k.includes('\u{FEFF}')),
            'no BOM in keys',
        );
        assert.ok(
            Object.keys(rows[0]).every((k) => !k.includes('"')),
            'no quotes in keys',
        );
    });

    it('reads CSV continuation without header when columnHeaders is provided', async () => {
        const p = path.join(temporary, 'cont.csv');
        await fs.writeFile(p, '3,4\n', 'utf8');
        const rows = await readRowsFromFile(p, 'csv', {
            columnHeaders: ['col1', 'col2'],
        });
        assert.deepEqual(rows, [{ col1: '3', col2: '4' }]);
    });
});

describe('streamRowsFromFile', () => {
    let temporary;
    before(async () => {
        temporary = await createFixtureRoot('mcdata-stream-');
    });
    // Keep run-owned fixtures for diagnostics; never recursively delete the run root.

    it('matches readRowsFromFile for CSV', async () => {
        const p = path.join(temporary, 's.csv');
        await fs.writeFile(p, 'col1,col2\n1,2\n', 'utf8');
        const expected = await readRowsFromFile(p, 'csv');
        const streamed = await collectAsync(streamRowsFromFile(p, 'csv'));
        assert.deepEqual(streamed, expected);
    });

    it('matches readRowsFromFile for JSON', async () => {
        const p = path.join(temporary, 's.json');
        await fs.writeFile(p, JSON.stringify([{ x: 1 }]), 'utf8');
        const expected = await readRowsFromFile(p, 'json');
        const streamed = await collectAsync(streamRowsFromFile(p, 'json'));
        assert.deepEqual(streamed, expected);
    });
});

describe('readRowsFromImportPaths', () => {
    let temporary;
    before(async () => {
        temporary = await createFixtureRoot('mcdata-read-multi-');
    });
    // Keep run-owned fixtures for diagnostics; never recursively delete the run root.

    it('concatenates two CSV parts (header in first file only)', async () => {
        const p1 = path.join(temporary, 'a.csv');
        const p2 = path.join(temporary, 'b.csv');
        await fs.writeFile(p1, 'col1,col2\n1,2\n', 'utf8');
        await fs.writeFile(p2, '3,4\n', 'utf8');
        const rows = await readRowsFromImportPaths([p1, p2], 'csv');
        assert.deepEqual(rows, [
            { col1: '1', col2: '2' },
            { col1: '3', col2: '4' },
        ]);
    });

    it('concatenates JSON arrays from multiple files', async () => {
        const p1 = path.join(temporary, 'x.json');
        const p2 = path.join(temporary, 'y.json');
        await fs.writeFile(p1, JSON.stringify([{ a: 1 }]), 'utf8');
        await fs.writeFile(p2, JSON.stringify([{ a: 2 }]), 'utf8');
        const rows = await readRowsFromImportPaths([p1, p2], 'json');
        assert.deepEqual(rows, [{ a: 1 }, { a: 2 }]);
    });

    it('streamRowsFromImportPaths matches readRowsFromImportPaths for multi-part CSV', async () => {
        const p1 = path.join(temporary, 'sa.csv');
        const p2 = path.join(temporary, 'sb.csv');
        await fs.writeFile(p1, 'col1,col2\n1,2\n', 'utf8');
        await fs.writeFile(p2, '3,4\n', 'utf8');
        const expected = await readRowsFromImportPaths([p1, p2], 'csv');
        const streamed = await collectAsync(streamRowsFromImportPaths([p1, p2], 'csv'));
        assert.deepEqual(streamed, expected);
    });

    it('countDataRowsFromImportPaths matches row array length', async () => {
        const p1 = path.join(temporary, 'cnt.csv');
        await fs.writeFile(p1, 'a,b\n1,2\n3,4\n', 'utf8');
        const rows = await readRowsFromImportPaths([p1], 'csv');
        const n = await countDataRowsFromImportPaths([p1], 'csv');
        assert.equal(n, rows.length);
    });
});
