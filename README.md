# sfmc-dataloader

Command-line tool **`mcdata`** to export and import **Salesforce Marketing Cloud Engagement** Data Extension rows using [sfmc-sdk](https://www.npmjs.com/package/sfmc-sdk) for REST/SOAP.

Works **standalone** — no mcdev installation required — and also integrates with existing [mcdev](https://github.com/Accenture/sfmc-devtools) projects.

## Config files

| File pair                              | When to use                                                       |
| -------------------------------------- | ----------------------------------------------------------------- |
| `.mcdevrc.json` + `.mcdev-auth.json`   | Existing mcdev projects — **always wins** when both pairs present |
| `.mcdatarc.json` + `.mcdata-auth.json` | Standalone setup; created by `mcdata init`                        |

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

- Node.js `^22.22.2 || >=24.15.0` (aligned with `sfmc-sdk`)
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

CSV/TSV/JSON exports stream rows with backpressure: slow file writes pause further production rather than accumulating the whole export in memory. Each output part is finished before moving to the next, and success is reported only after the final file write completes. A stream or API failure stops the export; partial output files may remain.

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

If an explicit `--file` selection contains more than one export run or format for the same DE, the import is rejected instead of silently choosing one. Automatic file discovery still chooses the newest run.

Imports use the **asynchronous** bulk row API only: `POST` for `--mode insert`, `PUT` for `--mode upsert` (same endpoint path). After uploading, `mcdata` waits 5 seconds before checking the first valid request ID, then checks request IDs sequentially without repeating that initial wait. Each request still waits 5 seconds between status checks while it is `Pending` or `Executing`, until it reaches a terminal status such as `Complete` or `Error`. On `Error`, per-row error messages are printed to the log so you can see exactly which records failed. The process exits with code 1 if any import job fails or completion cannot be confirmed (including missing request IDs, unavailable status responses, or unexpected statuses), which also surfaces as an error notification in the VS Code extension. Submitted work may still run after an unconfirmed result; no automatic rollback is performed.

Resolves the latest matching export file under `./data/MyCred/MyBU/` for that DE key. The file format is detected automatically from the file extension (`.csv`, `.tsv`, `.json`).

Import from explicit paths (the DE key is taken from the `.mcdata.` basename):

```bash
mcdata import MyCred/MyBU --file ./data/MyCred/MyBU/encoded%2Bkey.mcdata.2026-04-06T12-00-00.000Z.csv
```

#### Input validation and exact values

Use the import-only `--skip-empty-lines` flag to skip unquoted blank or whitespace-only CSV/TSV records. It is opt-in (default `false`) and does not trim values or remove quoted explicit empty cells, delimiter-only rows, quoted whitespace, or blank lines inside quoted multiline fields. JSON input is unaffected. The same validated parsing policy is reused for multipart files, cross-BU snapshots, and every upload; an input with no remaining data rows is rejected before backup, snapshot, CLEAR, or upload.

Before the first CLEAR or upload, a single-BU CLI import resolves and validates **all selected DEs and files**, including repeated `--file` inputs and imports without `--clear-before-import`. Cross-BU file imports validate all selected sources; API-mode cross-BU imports download and validate every requested source before any target is cleared or uploaded to.

- CSV/TSV files are scanned to the end with strict quote and column-count checks; duplicate column headers are rejected. A leading UTF-8 BOM is handled before quoted headers or continuation data. CSV syntax quotes are decoded without stripping literal quotes from headers or values; empty cells, Unicode, quoted separators and multiline values are preserved as strings.
- Selected multipart runs must start at `part1`, have contiguous, unique part numbers, and not mix numbered and unnumbered files. Every selected part must be readable. The first part supplies headers; later parts contain data only. A header-only first part is valid when later parts contain rows; a complete set with no data rows is rejected.
- Validation supplies the headers and total record count used for streaming import, avoiding a separate recount scan. CSV/TSV preflight does not retain all rows in memory. JSON inputs retain support for a row array or `{ "items": [...] }` and must also contain data.

Keep selected source files unchanged until the import finishes: preflight is not a snapshot or a lock against external edits. Backups are checked against the selected sources so the job cannot overwrite its own inputs, including through aliases or hard links. Cross-BU imports prepare all required target snapshots before the first target mutation, preserve logical records when converting multipart sources, and refuse to overwrite existing backup/snapshot destinations.

Preflight prevents known input/preparation failures from causing CLEAR or upload effects; it is **not a transaction or rollback guarantee**. Later API or operational failures can leave cleared targets and earlier accepted submissions. Failure diagnostics distinguish accepted rows/requests from confirmed completion; a failed request may also have reached the server. There is no automatic rollback or job-level restart, and async acceptance alone does not mean the rows completed successfully.

#### Payload sizing and synthetic benchmark

Payload chunks retain row order and respect both the 2,500-row cap and the 5,500,000-byte UTF-8 JSON budget; a single oversized row is kept alone. Plain JSON data is sized incrementally rather than repeatedly serializing growing prefixes. Programmatic inputs with accessors, custom prototypes, `toJSON`, proxies or unsupported JSON values retain the compatibility path and its serialization behavior.

Streaming imports flush their input window on the serialized byte budget or row limit and await each submission before consuming more input. Buffered serialized content is bounded by the budget plus one lookahead or oversized row, not a JavaScript heap cap. Progress reports numbered windows without an exact total because wide rows can add windows. Individual JSON input files are still parsed in full, so their memory use depends on the largest input part.

From a source checkout, run `npm run benchmark:batch --no-workspaces` to compare synthetic narrow, wide and Unicode rows at 250, 1,000 and 2,500 rows. Each case uses a fresh Node process and reports input bytes, chunk sizes, elapsed sizing time and process-lifetime peak RSS (KiB). Use the same Node version and machine for before/after comparisons; these are diagnostic measurements, not CI timing thresholds. No credentials, customer fixtures or API calls are used.

#### Upsert vs insert

- **Upsert** follows the platform's usual behaviour: update when a primary key matches, otherwise insert. For Data Extensions **without** a primary key, upsert **will fail**; use **`--mode insert`** for those.
- **Insert** always adds new rows. Running import twice with insert can create **duplicate** rows if the same file is applied again—use upsert when primary keys are defined and you need to ensure repeated runs always have the same outcome.

### Import — one source BU into multiple target BUs (API mode)

Use `--from` (one source) and `--to` (repeatable targets) for a cross-BU import where rows are fetched live from the source BU:

```bash
mcdata import --from MyCred/Dev --to MyCred/QA --to MyCred/Prod --de Contact_DE
```

An optional pre-import backup exports current target BU data as **timestamped** files (backup filenames always include the timestamp, regardless of `--git`). Use `--backup-before-import` to run the backup without a prompt (CI-safe), or `--no-backup-before-import` to skip it entirely. When neither flag is provided the CLI prompts interactively (TTY only). A snapshot file is also written to each target BU's data directory as a record of what was imported. Cross-BU snapshots stream JSON, CSV, and TSV output with backpressure, including format conversions; multipart uploads iterate parts in order instead of collecting the complete set. All target snapshots must finish successfully before any target CLEAR or upload. Each individual JSON input part is still parsed in full (no incremental JSON parser).

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

| Option                       | Description                                                                                                           |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `-p, --project`              | Project root (default: cwd)                                                                                           |
| `--format`                   | `csv` (default), `tsv`, or `json` — **export only**; import format is detected from the file extension                |
| `--git`                      | Stable export filenames: `<key>.mcdata.<ext>` (no timestamp segment)                                                  |
| `--mode`                     | `upsert` (default) or `insert` — async bulk REST API only                                                             |
| `--from <cred>/<bu>`         | Export: source BU (repeatable). Import API mode: single source BU (use with `--to` and `--de`)                        |
| `--to <cred>/<bu>`           | Import: target BU (repeatable). API mode: use with `--from`/`--de`. File mode: use with `--file` (no `--from` needed) |
| `--backup-before-import`     | Export target DE data as a timestamped backup before import (no prompt; always timestamped)                           |
| `--no-backup-before-import`  | Skip the backup prompt even in interactive (TTY) sessions                                                             |
| `--clear-before-import`      | SOAP `ClearData` before REST import                                                                                   |
| `--i-accept-clear-data-risk` | Non-interactive consent for clear                                                                                     |

Diagnostic logs mask `--client-id` and `--client-secret` values in the recorded command line, case-insensitive authorization headers, and SOAP authentication tokens. Treat logs as potentially sensitive: other request and response data can still contain business data.

Log lines include **row counts** and show file paths as **absolute paths in double-quotes** (e.g. `"C:\data\MyCred\DEV\Contact.mcdata.csv"`) so they are clickable in VS Code's integrated terminal.

## Programmatic API (Node.js)

Import from `sfmc-dataloader` for use in other tools (for example the **SFMC Data Loader** VS Code extension):

The internal file-reader helpers in `lib/read-rows.mjs` accept `{ skipEmptyLines: true }` to skip only unquoted blank or whitespace-only CSV/TSV records. The default is `false`; quoted empty/whitespace values, delimiter-only records and blank lines inside quoted fields remain data, and JSON is unaffected. Pass options as the third argument to `preflightImportPaths()` or the single-file read/stream helpers, and as the fourth argument to the multipart read/stream/count helpers (after the existing optional metadata argument). Preflight freezes the normalized boolean as `metadata.skipEmptyLines`; supplied metadata is authoritative, including when later options differ. Inputs must remain unchanged between preflight and consumption. The import CLI exposes this policy through `--skip-empty-lines`.

`importFromFile()` and `crossBuImport()` accept an optional `skipEmptyLines: true` parameter for file parsing (default `false`; ignored for JSON). `importFromFile()` validates the entire file before submitting rows and reuses the validated record count. The lower-level `importRowsStreamingForDe()` accepts an arbitrary row iterator; it does not promise file preflight. Both submission helpers report accepted requests, not confirmed asynchronous completion.

| Export                                     | Purpose                                                                                                                                                                                                                                |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `fetchDeList(projectRoot, credential, bu)` | Returns all Data Extension **names** and **customer keys** for one BU via SOAP `retrieveBulk` (pagination handled by **sfmc-sdk**). Not a CLI command — intended for in-process callers that already have mcdev/mcdata config on disk. |

`loadPublicProjectConfig(projectRoot, { stderr } = {})` asynchronously returns `{ mcdevrc }` for connection listing without reading auth-file contents. It requires a complete config/auth pair: mcdev takes precedence (and warns through `stderr` when mcdata files are ignored); an incomplete mcdev pair fails rather than falling back to mcdata. An incomplete mcdata pair or absent configuration also fails. Only the selected public JSON is parsed, so malformed auth JSON does not prevent listing.

`loadProjectConfig(projectRoot, { stderr } = {})` and `loadMcdevProject(projectRoot)` remain synchronous and return `{ mcdevrc, mcdevAuth }`. Malformed selected public/auth JSON produces a `SyntaxError` naming the file, without JSON excerpts or the original parser error. Successful CLI behavior is unchanged.

## License

MIT — Author: Jörn Berkefeld
