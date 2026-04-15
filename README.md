# sfmc-dataloader

Command-line tool **`mcdata`** to export and import **Salesforce Marketing Cloud Engagement** Data Extension rows using [sfmc-sdk](https://www.npmjs.com/package/sfmc-sdk) for REST/SOAP.

Works **standalone** — no mcdev installation required — and also integrates with existing [mcdev](https://github.com/Accenture/sfmc-devtools) projects.

## Config files

| File pair | When to use |
|---|---|
| `.mcdevrc.json` + `.mcdev-auth.json` | Existing mcdev projects — **always wins** when both pairs present |
| `.mcdatarc.json` + `.mcdata-auth.json` | Standalone setup; created by `mcdata init` |

**Precedence:** When `.mcdevrc.json` and `.mcdev-auth.json` both exist they are loaded; any mcdata files are ignored (a warning is printed to stderr). Both file pairs share the same logical shape (`credentials.<name>.businessUnits`), so all commands work with either layout.

## Standalone setup with `mcdata init`

If you don't have an existing mcdev project, run:

```bash
mcdata init
```

The interactive wizard collects:

1. Credential name (e.g. `MyOrg`)
2. Installed-package **Client ID**
3. Installed-package **Client Secret**
4. **Auth URL** (e.g. `https://<tenantsubdomain>.auth.marketingcloudapis.com/`)
5. **Enterprise MID** (parent account ID)

It fetches your Business Unit list via the SOAP API, writes `.mcdatarc.json` and `.mcdata-auth.json`, and adds the auth file to `.gitignore`.

### Non-interactive / CI mode

Pass all five flags to skip prompts:

```bash
mcdata init \
  --credential MyOrg \
  --client-id  <id> \
  --client-secret <secret> \
  --auth-url  https://<tenantsubdomain>.auth.marketingcloudapis.com/ \
  --enterprise-id <eid> \
  --yes
```

Use `--yes` to overwrite existing `.mcdatarc.json` / `.mcdata-auth.json` without confirmation.

## Requirements

- Node.js `^20.19.0 || ^22.13.0 || >=24` (aligned with `sfmc-sdk`)
- An SFMC installed package with Data Extension access
- **Optional:** `mcdev` `>=7` — listed in `optionalDependencies`; install globally if you also use mcdev for other metadata types

## Install

```bash
npm install -g sfmc-dataloader
```

## Usage

Run from your mcdev project root (where `.mcdevrc.json` lives).

### Export — single BU

```bash
mcdata export MyCred/MyBU --de MyDE_CustomerKey --format csv
```

Writes to `./data/MyCred/MyBU/<encodedKey>.mcdata.<timestamp>.csv` (TSV/JSON with `--format`). Timestamps use the same filesystem-safe ISO format as before.

Use **`--git`** for a stable name without a timestamp: `<encodedKey>.mcdata.csv` (useful for version control).

### Export — multiple BUs at once

Use `--from` (repeatable) instead of the positional argument to export the same DE(s) from several BUs in one command:

```bash
mcdata export --from MyCred/Dev --from MyCred/QA --de Contact_DE --de Order_DE
```

Creates one file per BU/DE combination using the same `.mcdata.` naming rules.

### Import — single BU

```bash
mcdata import MyCred/MyBU --de MyDE_CustomerKey --mode upsert
```

Imports use the **asynchronous** bulk row API only: `POST` for `--mode insert`, `PUT` for `--mode upsert` (same endpoint path). After uploading, `mcdata` polls the async status endpoint every 5 seconds until the job reaches `Complete` or `Error`. On `Error`, per-row error messages are printed to the log so you can see exactly which records failed. The process exits with code 1 if any import job fails, which also surfaces as an error notification in the VS Code extension.

Resolves the latest matching export file under `./data/MyCred/MyBU/` for that DE key. The file format is detected automatically from the file extension (`.csv`, `.tsv`, `.json`).

Import from explicit paths (the DE key is taken from the `.mcdata.` basename):

```bash
mcdata import MyCred/MyBU --file ./data/MyCred/MyBU/encoded%2Bkey.mcdata.2026-04-06T12-00-00.000Z.csv
```

#### Upsert vs insert

- **Upsert** follows the platform's usual behaviour: update when a primary key matches, otherwise insert. For Data Extensions **without** a primary key, upsert **will fail**; use **`--mode insert`** for those.
- **Insert** always adds new rows. Running import twice with insert can create **duplicate** rows if the same file is applied again—use upsert when primary keys are defined and you need to ensure repeated runs always have the same outcome.

### Import — one source BU into multiple target BUs (API mode)

Use `--from` (one source) and `--to` (repeatable targets) for a cross-BU import where rows are fetched live from the source BU:

```bash
mcdata import --from MyCred/Dev --to MyCred/QA --to MyCred/Prod --de Contact_DE
```

An optional pre-import backup exports current target BU data as **timestamped** files (backup filenames always include the timestamp, regardless of `--git`). Use `--backup-before-import` to run the backup without a prompt (CI-safe), or `--no-backup-before-import` to skip it entirely. When neither flag is provided the CLI prompts interactively (TTY only). A snapshot file is also written to each target BU's data directory as a record of what was imported.

### Import — local export files into multiple target BUs (file mode)

Use `--to` (repeatable targets) and `--file` (repeatable file paths) to push previously exported data files to multiple BUs without connecting to a source BU. The DE customer key is derived from each filename automatically:

```bash
mcdata import --to MyCred/QA --to MyCred/Prod \
  --file ./data/MyCred/Dev/Contact_DE.mcdata.2026-04-08T10-00-00.000Z.csv
```

Multiple files can be supplied to push several DEs in one command. Pass **`--git`** on export if you rely on stable `*.mcdata.<ext>` names for snapshots in this flow.

### Backup target DE before import

Use `--backup-before-import` on any import command (single-BU or cross-BU) to export a timestamped snapshot of the current target DE rows before the import runs. The backup filename always includes a timestamp regardless of whether `--git` is set.

```bash
mcdata import MyCred/MyBU --de MyKey --backup-before-import
```

In CI, combine with `--no-backup-before-import` to suppress any TTY prompt:

```bash
mcdata import MyCred/MyBU --de MyKey --no-backup-before-import
```

### Clear all rows before import

**Dangerous:** removes every row in the target Data Extension(s) before uploading.

Single-BU:

```bash
mcdata import MyCred/MyBU --de MyKey --clear-before-import
```

Cross-BU (warning lists every affected BU):

```bash
mcdata import --from MyCred/Dev --to MyCred/QA --to MyCred/Prod \
  --de Contact_DE --clear-before-import
```

Interactive: type `YES` when prompted. In CI, add `--i-accept-clear-data-risk` after reviewing the risk.

## Options

| Option                        | Description                                                                                                           |
| ----------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `-p, --project`               | Project root (default: cwd)                                                                                           |
| `--format`                    | `csv` (default), `tsv`, or `json` — **export only**; import format is detected from the file extension               |
| `--git`                       | Stable export filenames: `<key>.mcdata.<ext>` (no timestamp segment)                                                  |
| `--mode`                      | `upsert` (default) or `insert` — async bulk REST API only                                                             |
| `--from <cred>/<bu>`          | Export: source BU (repeatable). Import API mode: single source BU (use with `--to` and `--de`)                        |
| `--to <cred>/<bu>`            | Import: target BU (repeatable). API mode: use with `--from`/`--de`. File mode: use with `--file` (no `--from` needed) |
| `--backup-before-import`      | Export target DE data as a timestamped backup before import (no prompt; always timestamped)                           |
| `--no-backup-before-import`   | Skip the backup prompt even in interactive (TTY) sessions                                                             |
| `--clear-before-import`       | SOAP `ClearData` before REST import                                                                                   |
| `--i-accept-clear-data-risk`  | Non-interactive consent for clear                                                                                     |

Log lines include **row counts** and show file paths as **absolute paths in double-quotes** (e.g. `"C:\data\MyCred\DEV\Contact.mcdata.csv"`) so they are clickable in VS Code's integrated terminal.

## License

MIT — Author: Jörn Berkefeld
