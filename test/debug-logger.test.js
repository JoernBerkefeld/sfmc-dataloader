import assert from 'node:assert/strict';
import { describe, it, before, after } from 'node:test';
import fs from 'node:fs/promises';
import fsSync from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { initDebugLogger } from '../lib/debug-logger.mjs';

describe('initDebugLogger', () => {
    let tmp;
    before(async () => {
        tmp = await fs.mkdtemp(path.join(os.tmpdir(), 'mcdata-debug-'));
    });

    after(async () => {
        await fs.rm(tmp, { recursive: true, force: true });
    });

    it('creates logs/data directory', () => {
        const logger = initDebugLogger(tmp, '1.2.3', ['node', 'mcdata.mjs', 'export', 'R1/DEV']);
        assert.ok(fsSync.existsSync(path.join(tmp, 'logs', 'data')));
        assert.ok(fsSync.existsSync(logger.logPath));
    });

    it('creates log file with timestamped name', () => {
        const logger = initDebugLogger(tmp, '1.2.3', ['node', 'mcdata.mjs', 'export', 'R1/DEV']);
        const basename = path.basename(logger.logPath);
        assert.ok(basename.endsWith('.log'), 'should end with .log');
        assert.ok(basename.includes('T'), 'should contain ISO timestamp T separator');
        assert.ok(basename.includes('.'), 'should use dots instead of colons');
    });

    it('writes header with version and command', async () => {
        const logger = initDebugLogger(tmp, '2.0.0', [
            'node',
            'mcdata.mjs',
            'import',
            'R1/DEV',
            '--de',
            'MyDE',
        ]);
        const content = await fs.readFile(logger.logPath, 'utf8');
        assert.ok(content.includes('mcdata v2.0.0'), 'header should include version');
        assert.ok(content.includes('Ran command: mcdata import R1/DEV --de MyDE'));
        assert.ok(content.includes('---'), 'header should end with separator');
    });

    it('quotes arguments with spaces in header', async () => {
        const logger = initDebugLogger(tmp, '2.0.0', [
            'node',
            'mcdata.mjs',
            'import',
            'R1/DEV',
            '--file',
            String.raw`c:\path with spaces\file.csv`,
        ]);
        const content = await fs.readFile(logger.logPath, 'utf8');
        assert.ok(
            content.includes(String.raw`"c:\path with spaces\file.csv"`),
            'args with spaces should be quoted',
        );
    });

    it('write() appends lines to log file', async () => {
        const logger = initDebugLogger(tmp, '1.0.0', ['node', 'mcdata.mjs', 'export', 'R1/DEV']);
        logger.write('API REQUEST >> GET /test');
        logger.write('API RESPONSE << 200');
        const content = await fs.readFile(logger.logPath, 'utf8');
        assert.ok(content.includes('API REQUEST >> GET /test'));
        assert.ok(content.includes('API RESPONSE << 200'));
    });
});
