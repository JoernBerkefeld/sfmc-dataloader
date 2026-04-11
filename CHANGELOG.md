# Changelog

All notable changes to sfmc-dataloader will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/), and this project adheres to [Semantic Versioning](https://semver.org/).

## [2.2.0] — 2026-04-11

### Added

- `--backup-before-import` flag: exports a timestamped snapshot of each target DE before import — no prompt, safe in CI
- `--no-backup-before-import` flag: skips the pre-import backup prompt even in interactive (TTY) sessions
- Pre-import backup support for single-BU import (`mcdata import <cred/bu> --de …`) — previously only cross-BU had backup

### Changed

- `--format` now applies to **exports only**; import format is always auto-detected from file extension
- Snapshot files written during cross-BU import now always use a timestamped filename regardless of `--git`
- Upsert on Data Extensions without a primary key now explicitly documented as failing — use `--mode insert` for those
- Fixed test glob pattern (`test/**/*.test.js` → `test/*.test.js`) to work on Node 20.19 without `globstar`
