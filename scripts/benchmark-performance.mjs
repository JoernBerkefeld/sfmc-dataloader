import { spawnSync } from 'node:child_process';
import { performance } from 'node:perf_hooks';
import { chunkItemsForPayload } from '../lib/batch.mjs';

// Each synthetic case runs in a fresh process; no customer data or API calls.
const [kind, count] = process.argv.slice(2);
if (kind) {
    const values = { narrow: 'abc', wide: 'x'.repeat(3000), unicode: '😀漢é'.repeat(100) };
    const rows = Array.from({ length: Number(count) }, (_, id) => ({ id, value: values[kind] }));
    const inputBytes = Buffer.byteLength(JSON.stringify({ items: rows }));
    const start = performance.now();
    const chunks = chunkItemsForPayload(rows);
    const milliseconds = performance.now() - start;
    const result = {
        kind,
        rows: rows.length,
        inputBytes,
        chunks: chunks.map((c) => c.length),
        milliseconds: Number(milliseconds.toFixed(3)),
        peakRssKiB: process.resourceUsage().maxRSS,
    };
    process.stdout.write(`${JSON.stringify(result)}\n`);
} else {
    process.stdout.write(
        `Node ${process.version}; milliseconds and peak RSS KiB (process lifetime)\n`,
    );
    for (const shape of ['narrow', 'wide', 'unicode']) {
        for (const size of [250, 1000, 2500]) {
            const result = spawnSync(
                process.execPath,
                [import.meta.filename, shape, String(size)],
                {
                    encoding: 'utf8',
                },
            );
            if (result.status !== 0) {
                throw new Error(result.stderr || 'Benchmark child failed');
            }
            process.stdout.write(result.stdout);
        }
    }
}
