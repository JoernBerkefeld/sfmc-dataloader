#!/usr/bin/env node
import { main } from '../lib/cli.mjs';

main(process.argv)
    .then((code) => process.exit(typeof code === 'number' ? code : 0))
    .catch((ex) => {
        console.error(ex.message ?? String(ex));
        process.exit(1);
    });
