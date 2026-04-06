#!/usr/bin/env node
import { main } from '../lib/cli.mjs';

main(process.argv)
    .then((code) => process.exit(typeof code === 'number' ? code : 0))
    .catch((err) => {
        console.error(err);
        process.exit(1);
    });
