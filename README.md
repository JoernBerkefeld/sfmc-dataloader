# sfmc-dataloader

Command-line tool **`mcdata`** to export and import Salesforce Marketing Cloud Data Extension rows using the same project files as [mcdev](https://github.com/Accenture/sfmc-devtools) (`.mcdevrc.json`, `.mcdev-auth.json`) and [sfmc-sdk](https://www.npmjs.com/package/sfmc-sdk) for REST/SOAP.

## Requirements

- Node.js `^20.19.0 || ^22.13.0 || >=24` (aligned with `sfmc-sdk`)
- A mcdev-style project with credentials on disk
- Peer: `mcdev` `>=7` (declare alongside your project tooling)

## Install

```bash
npm install -g sfmc-dataloader
```

## Usage

Run from your mcdev project root (where `.mcdevrc.json` lives).

### Export

```bash
mcdata export MyCred/MyBU --de MyDE_CustomerKey --format csv
```

Writes to `./data/MyCred/MyBU/<encodedKey>+MCDATA+<timestamp>.csv` (TSV/JSON with `--format`).

### Import

```bash
mcdata import MyCred/MyBU --de MyDE_CustomerKey --format csv --api async --mode upsert
```

Resolves the latest matching export file under `./data/MyCred/MyBU/` for that DE key.

Import from explicit paths (DE key is recovered from the `+MCDATA+` filename):

```bash
mcdata import MyCred/MyBU --file ./data/MyCred/MyBU/encoded%2Bkey+MCDATA+2026-04-06T12-00-00.000Z.csv
```

### Clear all rows before import

**Dangerous:** removes every row in the target Data Extension before uploading.

```bash
mcdata import MyCred/MyBU --de MyKey --clear-before-import
```

Interactive: type `YES` when prompted. In CI, add `--i-accept-clear-data-risk` after reviewing the risk.

## Options

| Option | Description |
|--------|-------------|
| `-p, --project` | Project root (default: cwd) |
| `--format` | `csv` (default), `tsv`, or `json` |
| `--api` | `async` (default) or `sync` |
| `--mode` | `upsert` (default), `insert` and `update` require `--api sync` |
| `--clear-before-import` | SOAP `ClearData` before REST import |
| `--i-accept-clear-data-risk` | Non-interactive consent for clear |

## License

MIT — Author: Jörn Berkefeld
