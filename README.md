# cia-unix-sync

A Rust CLI that wraps the `cia-unix` toolchain with a better local workflow for decrypting 3DS and CIA dumps from a folder over time

It adds:

- Interactive folder selection, with an option to remember the directory for next time
- Automatic installation of the helper tools this workflow needs
- Tracking for files that have already been processed so reruns only pick up new dumps
- Readable logs for each sync run
- Original preservation by default, with decrypted files written back to your folder

## Quick Start

Build the CLI:

```sh
cargo build
```

Install the required external tools:

```sh
cargo run -- install-tools
```

Run the normal interactive sync flow:

```sh
cargo run -- sync
```

## Commands

Use `cargo run --` during development, or run the built binary directly from `./target/debug/cia-unix-sync`

Install helper tools into `./bin`:

```sh
cargo run -- install-tools
```

Run interactively using the remembered folder or the folder picker:

```sh
cargo run -- sync
```

Run against a specific folder:

```sh
cargo run -- sync --folder /path/to/folder
```

Skip the confirmation prompt:

```sh
cargo run -- sync --yes
```

Do not keep originals:

```sh
cargo run -- sync --yolo
```

Use a specific folder and skip the confirmation prompt:

```sh
cargo run -- sync --folder /path/to/folder --yes
```

## Behavior

- Only `.3ds` and `.cia` input files are scanned
- Successfully processed files are tracked by relative path inside the selected folder
- By default, originals are moved into `originals/` under the folder
- `--yolo` disables original preservation and replaces or removes originals directly

## Local Files

- Tool binaries: `./bin`
- Human-readable logs: `./logs`
- State: `./state.json`
- Remembered folder config: `./config.json`
- Temporary staging: system temp directory

## Notes

- Tool installation currently supports macOS and Linux `x86_64`
- The actual decrypt/build work still depends on external tools: `ctrdecrypt`, `ctrtool`, `makerom`, and `seeddb.bin`

## Credit

This project is based on the original `cia-unix` work by shijimasoft, with this repo adding a Rust CLI wrapper and some quality-of-life improvements around folder syncing, state, logging, and originals management
