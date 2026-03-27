use std::collections::HashSet;
use std::env;
use std::ffi::OsStr;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use clap::{Parser, Subcommand};
use dialoguer::Confirm;
use regex::Regex;
use rfd::FileDialog;
use serde::{Deserialize, Serialize};
use tempfile::{Builder, TempDir};
use walkdir::WalkDir;
use zip::ZipArchive;

const CTRDECRYPT_VER: &str = "1.1.0";
const CTRTOOL_VER: &str = "1.2.0";
const MAKEROM_VER: &str = "0.18.4";
const SEEDDB_URL: &str = "https://github.com/ihaveamac/3DS-rom-tools/raw/master/seeddb/seeddb.bin";

#[derive(Parser)]
#[command(name = "cia-unix-sync")]
#[command(about = "Sync and decrypt 3DS/CIA files using the cia-unix toolchain.")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    InstallTools,
    Sync {
        #[arg(long = "folder")]
        roms_folder: Option<PathBuf>,
        #[arg(long)]
        yes: bool,
        #[arg(long, help = "Do not keep originals; replace/remove them directly.")]
        yolo: bool,
    },
}

#[derive(Debug)]
struct AppPaths {
    repo_root: PathBuf,
    bin_dir: PathBuf,
    logs_dir: PathBuf,
    state_file: PathBuf,
    config_file: PathBuf,
    sync_log: PathBuf,
    run_logs_dir: PathBuf,
    ctrdecrypt: PathBuf,
    ctrtool: PathBuf,
    makerom: PathBuf,
    seeddb: PathBuf,
}

#[derive(Debug, Clone)]
struct CandidateFile {
    source_path: PathBuf,
    source_rel_path: PathBuf,
    source_name: String,
    source_size: u64,
    kind: RomKind,
}

#[derive(Debug, Clone, Copy)]
enum RomKind {
    ThreeDs,
    Cia,
}

#[derive(Debug, Clone, Copy)]
enum CiaKind {
    Game,
    Patch,
    Dlc,
}

#[derive(Debug)]
struct ProcessOutcome {
    output_name: String,
    output_size: u64,
    final_rel_path: PathBuf,
    final_name: String,
    archived_original_rel_path: Option<PathBuf>,
    run_log_path: PathBuf,
}

#[derive(Debug, Serialize, Deserialize)]
struct StateFile {
    version: u32,
    records: Vec<ProcessRecord>,
}

impl Default for StateFile {
    fn default() -> Self {
        Self {
            version: 1,
            records: Vec::new(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct ProcessRecord {
    status: String,
    processed_at: String,
    #[serde(default)]
    roms_folder: Option<String>,
    source_rel_path: String,
    source_name: String,
    source_size: u64,
    output_name: Option<String>,
    output_size: Option<u64>,
    final_rel_path: Option<String>,
    final_name: Option<String>,
    archived_original_rel_path: Option<String>,
    run_log_path: String,
    error: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct AppConfig {
    last_roms_folder: Option<String>,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            last_roms_folder: None,
        }
    }
}

#[derive(Debug)]
struct ToolDownload {
    destination: PathBuf,
    url: String,
    zipped_entry_suffix: Option<&'static str>,
    executable: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let paths =
        AppPaths::new(env::current_dir().context("failed to determine current directory")?)?;

    match cli.command {
        Commands::InstallTools => {
            install_tools(&paths)?;
        }
        Commands::Sync {
            roms_folder,
            yes,
            yolo,
        } => {
            let roms_dir = resolve_roms_folder(&paths, roms_folder)?;
            sync_folder(&paths, &roms_dir, yes, yolo)?;
        }
    }

    Ok(())
}

impl AppPaths {
    fn new(repo_root: PathBuf) -> Result<Self> {
        let repo_root = repo_root
            .canonicalize()
            .context("failed to canonicalize repo root")?;
        let bin_dir = repo_root.join("bin");
        let logs_dir = repo_root.join("logs");
        let legacy_state_dir = repo_root.join(".cia-unix-sync");
        let state_file = repo_root.join("state.json");
        let config_file = repo_root.join("config.json");
        let sync_log = logs_dir.join("sync.log");
        let run_logs_dir = logs_dir.join("run-logs");

        fs::create_dir_all(&bin_dir).context("failed to create bin directory")?;
        fs::create_dir_all(&logs_dir).context("failed to create logs directory")?;
        fs::create_dir_all(&run_logs_dir).context("failed to create run log directory")?;
        migrate_legacy_tool_paths(&repo_root, &bin_dir)?;
        migrate_legacy_log_paths(&legacy_state_dir, &logs_dir, &sync_log, &run_logs_dir)?;
        migrate_legacy_state_paths(&legacy_state_dir, &state_file, &config_file)?;

        Ok(Self {
            ctrdecrypt: bin_dir.join(binary_name("ctrdecrypt")),
            ctrtool: bin_dir.join(binary_name("ctrtool")),
            makerom: bin_dir.join(binary_name("makerom")),
            seeddb: bin_dir.join("seeddb.bin"),
            bin_dir,
            logs_dir,
            repo_root,
            state_file,
            config_file,
            sync_log,
            run_logs_dir,
        })
    }

    fn relative_to_repo(&self, path: &Path) -> String {
        path.strip_prefix(&self.repo_root)
            .unwrap_or(path)
            .to_string_lossy()
            .into_owned()
    }
}

fn migrate_legacy_tool_paths(repo_root: &Path, bin_dir: &Path) -> Result<()> {
    for file_name in [
        binary_name("ctrdecrypt"),
        binary_name("ctrtool"),
        binary_name("makerom"),
        "seeddb.bin".to_string(),
    ] {
        let legacy_path = repo_root.join(&file_name);
        let managed_path = bin_dir.join(&file_name);

        if managed_path.exists() || !legacy_path.exists() {
            continue;
        }

        fs::rename(&legacy_path, &managed_path).with_context(|| {
            format!(
                "failed to move legacy tool {} to {}",
                legacy_path.display(),
                managed_path.display()
            )
        })?;
    }

    Ok(())
}

fn migrate_legacy_log_paths(
    legacy_state_dir: &Path,
    logs_dir: &Path,
    sync_log: &Path,
    run_logs_dir: &Path,
) -> Result<()> {
    let legacy_sync_log = legacy_state_dir.join("sync.log");
    if legacy_sync_log.exists() && !sync_log.exists() {
        fs::rename(&legacy_sync_log, sync_log).with_context(|| {
            format!(
                "failed to move legacy log {} to {}",
                legacy_sync_log.display(),
                sync_log.display()
            )
        })?;
    }

    let legacy_run_logs_dir = legacy_state_dir.join("run-logs");
    if legacy_run_logs_dir.exists() {
        fs::create_dir_all(logs_dir).context("failed to create logs directory")?;
        fs::create_dir_all(run_logs_dir).context("failed to create run log directory")?;
        for entry in fs::read_dir(&legacy_run_logs_dir).with_context(|| {
            format!(
                "failed to read legacy run log directory {}",
                legacy_run_logs_dir.display()
            )
        })? {
            let entry = entry.with_context(|| {
                format!(
                    "failed to inspect legacy run log directory {}",
                    legacy_run_logs_dir.display()
                )
            })?;
            let target = run_logs_dir.join(entry.file_name());
            if target.exists() {
                continue;
            }
            fs::rename(entry.path(), &target).with_context(|| {
                format!(
                    "failed to move legacy run log {} to {}",
                    entry.path().display(),
                    target.display()
                )
            })?;
        }
    }

    Ok(())
}

fn migrate_legacy_state_paths(
    legacy_state_dir: &Path,
    state_file: &Path,
    config_file: &Path,
) -> Result<()> {
    let legacy_state_file = legacy_state_dir.join("state.json");
    if legacy_state_file.exists() && !state_file.exists() {
        fs::rename(&legacy_state_file, state_file).with_context(|| {
            format!(
                "failed to move legacy state {} to {}",
                legacy_state_file.display(),
                state_file.display()
            )
        })?;
    }

    let legacy_config_file = legacy_state_dir.join("config.json");
    if legacy_config_file.exists() && !config_file.exists() {
        fs::rename(&legacy_config_file, config_file).with_context(|| {
            format!(
                "failed to move legacy config {} to {}",
                legacy_config_file.display(),
                config_file.display()
            )
        })?;
    }

    Ok(())
}

fn sync_folder(paths: &AppPaths, roms_dir: &Path, yes: bool, yolo: bool) -> Result<()> {
    ensure_tools(paths)?;

    let roms_dir = roms_dir
        .canonicalize()
        .with_context(|| format!("failed to resolve {}", roms_dir.display()))?;
    if !roms_dir.is_dir() {
        bail!("{} is not a directory", roms_dir.display());
    }

    log_line(paths, format!("Scanning {}", roms_dir.display()))?;
    println!("Scanning {} for .3ds and .cia files...", roms_dir.display());
    let mut state = load_state(paths)?;
    let processed_paths = successful_paths_for(&state, &roms_dir);
    let pending = collect_pending_files(paths, &roms_dir, &processed_paths)?;

    if pending.is_empty() {
        println!("No new .3ds or .cia files found to decrypt.");
        log_line(paths, "No new files found".to_string())?;
        return Ok(());
    }

    println!(
        "Found {} file(s) not yet decrypted in {}:",
        pending.len(),
        roms_dir.display()
    );
    for candidate in &pending {
        println!("  {}", candidate.source_rel_path.display());
    }

    if !yes {
        let confirmed = Confirm::new()
            .with_prompt("Decrypt these now?")
            .default(true)
            .interact()
            .context("failed to read confirmation prompt")?;
        if !confirmed {
            println!("Aborted.");
            log_line(
                paths,
                "User aborted sync after confirmation prompt".to_string(),
            )?;
            return Ok(());
        }
    }

    let mut failures = Vec::new();
    for candidate in pending {
        match process_candidate(paths, &roms_dir, &candidate, yolo) {
            Ok(outcome) => {
                state.records.push(ProcessRecord {
                    status: "success".to_string(),
                    processed_at: iso_timestamp(),
                    roms_folder: Some(roms_dir.to_string_lossy().into_owned()),
                    source_rel_path: candidate.source_rel_path.to_string_lossy().into_owned(),
                    source_name: candidate.source_name,
                    source_size: candidate.source_size,
                    output_name: Some(outcome.output_name.clone()),
                    output_size: Some(outcome.output_size),
                    final_rel_path: Some(outcome.final_rel_path.to_string_lossy().into_owned()),
                    final_name: Some(outcome.final_name.clone()),
                    archived_original_rel_path: outcome
                        .archived_original_rel_path
                        .map(|path| path.to_string_lossy().into_owned()),
                    run_log_path: paths.relative_to_repo(&outcome.run_log_path),
                    error: None,
                });
                save_state(paths, &state)?;
            }
            Err(error) => {
                let error_text = format!("{error:#}");
                failures.push((candidate.source_rel_path.clone(), error_text.clone()));
                state.records.push(ProcessRecord {
                    status: "failed".to_string(),
                    processed_at: iso_timestamp(),
                    roms_folder: Some(roms_dir.to_string_lossy().into_owned()),
                    source_rel_path: candidate.source_rel_path.to_string_lossy().into_owned(),
                    source_name: candidate.source_name,
                    source_size: candidate.source_size,
                    output_name: None,
                    output_size: None,
                    final_rel_path: None,
                    final_name: None,
                    archived_original_rel_path: None,
                    run_log_path: paths
                        .relative_to_repo(&run_log_path_for(paths, &candidate.source_rel_path)),
                    error: Some(error_text),
                });
                save_state(paths, &state)?;
            }
        }
    }

    if failures.is_empty() {
        println!("Finished successfully.");
        log_line(paths, "Finished successfully".to_string())?;
        Ok(())
    } else {
        eprintln!("Finished with {} failure(s):", failures.len());
        for (rel_path, error) in &failures {
            eprintln!("  {}: {}", rel_path.display(), error);
        }
        log_line(
            paths,
            format!("Finished with {} failure(s)", failures.len()),
        )?;
        bail!("one or more files failed");
    }
}

fn resolve_roms_folder(paths: &AppPaths, provided: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(roms_folder) = provided {
        return canonical_roms_folder(&roms_folder);
    }

    let mut config = load_config(paths)?;
    if let Some(last_roms_folder) = config.last_roms_folder.clone() {
        let use_last = Confirm::new()
            .with_prompt(format!("Use last known path: {last_roms_folder}?"))
            .default(true)
            .interact()
            .context("failed to read remembered roms path prompt")?;
        if use_last {
            let last_path = PathBuf::from(&last_roms_folder);
            if last_path.is_dir() {
                return canonical_roms_folder(&last_path);
            }

            println!("Last known path is no longer available: {last_roms_folder}");
            config.last_roms_folder = None;
            save_config(paths, &config)?;
        }
    }

    let Some(selected_folder) = FileDialog::new()
        .set_title("Select folder")
        .pick_folder()
    else {
        bail!("no folder selected");
    };
    let roms_folder = canonical_roms_folder(&selected_folder)?;

    let remember = Confirm::new()
        .with_prompt("Remember this folder for next time?")
        .default(true)
        .interact()
        .context("failed to read remember roms path prompt")?;
    if remember {
        config.last_roms_folder = Some(roms_folder.to_string_lossy().into_owned());
        save_config(paths, &config)?;
    }

    Ok(roms_folder)
}

fn canonical_roms_folder(roms_folder: &Path) -> Result<PathBuf> {
    let roms_folder = roms_folder
        .canonicalize()
        .with_context(|| format!("failed to resolve {}", roms_folder.display()))?;
    if !roms_folder.is_dir() {
        bail!("{} is not a directory", roms_folder.display());
    }
    Ok(roms_folder)
}

fn collect_pending_files(
    paths: &AppPaths,
    roms_dir: &Path,
    processed_paths: &HashSet<String>,
) -> Result<Vec<CandidateFile>> {
    let originals_dir = roms_dir.join("originals");
    let mut pending = Vec::new();

    for entry in WalkDir::new(roms_dir)
        .into_iter()
        .filter_entry(|entry| entry.path() != originals_dir)
    {
        let entry = entry.with_context(|| format!("failed to read {}", roms_dir.display()))?;
        let path = entry.path();
        if !entry.file_type().is_file() {
            continue;
        }

        let Some(kind) = rom_kind_from_path(path) else {
            continue;
        };

        let rel_path = path
            .strip_prefix(roms_dir)
            .with_context(|| format!("failed to relativize {}", path.display()))?
            .to_path_buf();
        let source_name = file_name_string(path)?;

        let rel_path_text = rel_path.to_string_lossy().into_owned();
        if processed_paths.contains(&rel_path_text) {
            log_line(
                paths,
                format!(
                    "Skipping {} because it is already recorded as processed in state.json",
                    rel_path.display()
                ),
            )?;
            continue;
        }

        let source_size = file_size(path)?;
        pending.push(CandidateFile {
            source_path: path.to_path_buf(),
            source_rel_path: rel_path,
            source_name,
            source_size,
            kind,
        });
    }

    pending.sort_by(|left, right| left.source_rel_path.cmp(&right.source_rel_path));
    Ok(pending)
}

fn process_candidate(
    paths: &AppPaths,
    roms_dir: &Path,
    candidate: &CandidateFile,
    yolo: bool,
) -> Result<ProcessOutcome> {
    let run_log_path = run_log_path_for(paths, &candidate.source_rel_path);
    let mut run_log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&run_log_path)
        .with_context(|| format!("failed to open {}", run_log_path.display()))?;
    let stage_dir = Builder::new()
        .prefix("run.")
        .tempdir()
        .context("failed to create staging directory")?;

    log_line(
        paths,
        format!("Processing {}", candidate.source_rel_path.display()),
    )?;
    writeln!(run_log, "=== {} ===", iso_timestamp())?;
    writeln!(run_log, "Source: {}", candidate.source_path.display())?;

    let stage_input_path = stage_dir.path().join(&candidate.source_name);
    fs::copy(&candidate.source_path, &stage_input_path).with_context(|| {
        format!(
            "failed to copy {} into staging",
            candidate.source_path.display()
        )
    })?;

    let output_stage_path = match candidate.kind {
        RomKind::ThreeDs => process_3ds(paths, &stage_dir, &stage_input_path, &mut run_log)?,
        RomKind::Cia => process_cia(paths, &stage_dir, &stage_input_path, &mut run_log)?,
    };

    let output_name = file_name_string(&output_stage_path)?;
    let output_size = file_size(&output_stage_path)?;
    let (final_rel_path, final_name, archived_original_rel_path) = finalize_output(
        roms_dir,
        &candidate.source_path,
        &candidate.source_rel_path,
        &output_stage_path,
        !yolo,
    )?;

    writeln!(
        run_log,
        "Final path: {}",
        roms_dir.join(&final_rel_path).display()
    )?;

    Ok(ProcessOutcome {
        output_name,
        output_size,
        final_rel_path,
        final_name,
        archived_original_rel_path,
        run_log_path,
    })
}

fn process_3ds(
    paths: &AppPaths,
    stage_dir: &TempDir,
    stage_input_path: &Path,
    run_log: &mut File,
) -> Result<PathBuf> {
    let source_name = file_name_string(stage_input_path)?;
    let stem = stem_string(stage_input_path)?;

    run_command(
        &paths.ctrdecrypt,
        &[source_name.clone()],
        stage_dir.path(),
        run_log,
    )?;

    let mut makerom_args = vec![
        "-f".to_string(),
        "cci".to_string(),
        "-ignoresign".to_string(),
        "-target".to_string(),
        "p".to_string(),
        "-o".to_string(),
        format!("{stem}-decrypted.3ds"),
    ];

    let mut ncch_files = collect_ncch_files(stage_dir.path(), &stem)?;
    ncch_files.sort();
    for file_name in ncch_files {
        let part_idx = ncsd_partition_index(&stem, &file_name)?;
        makerom_args.push("-i".to_string());
        makerom_args.push(format!("{file_name}:{part_idx}:{part_idx}"));
    }

    run_command(&paths.makerom, &makerom_args, stage_dir.path(), run_log)?;

    let output = stage_dir.path().join(format!("{stem}-decrypted.3ds"));
    if !output.exists() {
        bail!(
            "expected decrypted output {} was not created",
            output.display()
        );
    }
    Ok(output)
}

fn process_cia(
    paths: &AppPaths,
    stage_dir: &TempDir,
    stage_input_path: &Path,
    run_log: &mut File,
) -> Result<PathBuf> {
    let source_name = file_name_string(stage_input_path)?;
    let stem = stem_string(stage_input_path)?;
    let ctrtool_output = run_command(
        &paths.ctrtool,
        &[
            format!("--seeddb={}", paths.seeddb.display()),
            source_name.clone(),
        ],
        stage_dir.path(),
        run_log,
    )?;
    let cia_kind = classify_cia(&ctrtool_output)?;

    run_command(&paths.ctrdecrypt, &[source_name], stage_dir.path(), run_log)?;

    match cia_kind {
        CiaKind::Game => {
            let mut makerom_args = vec![
                "-f".to_string(),
                "cia".to_string(),
                "-ignoresign".to_string(),
                "-target".to_string(),
                "p".to_string(),
                "-o".to_string(),
                format!("{stem}-decfirst.cia"),
            ];

            let mut ncch_files = collect_all_ncch(stage_dir.path())?;
            ncch_files.sort();
            for (index, file_name) in ncch_files.into_iter().enumerate() {
                makerom_args.push("-i".to_string());
                makerom_args.push(format!("{file_name}:{index}:{index}"));
            }

            run_command(&paths.makerom, &makerom_args, stage_dir.path(), run_log)?;

            let decfirst_name = format!("{stem}-decfirst.cia");
            run_command(
                &paths.makerom,
                &[
                    "-ciatocci".to_string(),
                    decfirst_name,
                    "-o".to_string(),
                    format!("{stem}-decrypted.cci"),
                ],
                stage_dir.path(),
                run_log,
            )?;

            let output = stage_dir.path().join(format!("{stem}-decrypted.cci"));
            if !output.exists() {
                bail!(
                    "expected decrypted output {} was not created",
                    output.display()
                );
            }
            Ok(output)
        }
        CiaKind::Patch | CiaKind::Dlc => {
            let suffix = match cia_kind {
                CiaKind::Patch => "(Patch)-decrypted.cia",
                CiaKind::Dlc => "(DLC)-decrypted.cia",
                CiaKind::Game => unreachable!(),
            };

            let mut makerom_args = vec!["-f".to_string(), "cia".to_string()];
            if matches!(cia_kind, CiaKind::Dlc) {
                makerom_args.push("-dlc".to_string());
            }
            makerom_args.extend([
                "-ignoresign".to_string(),
                "-target".to_string(),
                "p".to_string(),
                "-o".to_string(),
                format!("{stem} {suffix}"),
            ]);

            for (partition, file_name) in collect_partitioned_ncch(stage_dir.path(), &stem)? {
                makerom_args.push("-i".to_string());
                makerom_args.push(format!("{file_name}:{partition}:{partition}"));
            }

            run_command(&paths.makerom, &makerom_args, stage_dir.path(), run_log)?;

            let output = stage_dir.path().join(format!("{stem} {suffix}"));
            if !output.exists() {
                bail!(
                    "expected decrypted output {} was not created",
                    output.display()
                );
            }
            Ok(output)
        }
    }
}

fn finalize_output(
    roms_dir: &Path,
    source_path: &Path,
    source_rel_path: &Path,
    output_stage_path: &Path,
    keep_originals: bool,
) -> Result<(PathBuf, String, Option<PathBuf>)> {
    let output_name = file_name_string(output_stage_path)?;
    let output_ext = output_stage_path
        .extension()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let source_ext = source_path
        .extension()
        .and_then(OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let dest_dir = source_path
        .parent()
        .ok_or_else(|| anyhow!("{} has no parent directory", source_path.display()))?;

    let (final_abs_path, final_rel_path) = if output_ext == source_ext {
        (source_path.to_path_buf(), source_rel_path.to_path_buf())
    } else {
        let final_abs_path = dest_dir.join(&output_name);
        let final_rel_path = source_rel_path
            .parent()
            .map(|parent| parent.join(&output_name))
            .unwrap_or_else(|| PathBuf::from(&output_name));
        (final_abs_path, final_rel_path)
    };

    let archive_target = if keep_originals {
        let originals_root = roms_dir.join("originals");
        let archive_target = unique_archive_path(&originals_root, source_rel_path)?;
        if let Some(parent) = archive_target.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        Some(archive_target)
    } else {
        None
    };

    if output_ext == source_ext {
        let temp_output = temp_copy_path(&final_abs_path);
        fs::copy(output_stage_path, &temp_output).with_context(|| {
            format!(
                "failed to copy {} to {}",
                output_stage_path.display(),
                temp_output.display()
            )
        })?;
        if let Some(archive_target) = &archive_target {
            fs::copy(source_path, archive_target).with_context(|| {
                format!(
                    "failed to archive original {} to {}",
                    source_path.display(),
                    archive_target.display()
                )
            })?;
        }
        fs::rename(&temp_output, &final_abs_path).with_context(|| {
            format!(
                "failed to replace {} with {}",
                final_abs_path.display(),
                temp_output.display()
            )
        })?;
    } else {
        fs::copy(output_stage_path, &final_abs_path).with_context(|| {
            format!(
                "failed to copy {} to {}",
                output_stage_path.display(),
                final_abs_path.display()
            )
        })?;
        if let Some(archive_target) = &archive_target {
            fs::rename(source_path, archive_target).with_context(|| {
                format!(
                    "failed to move original {} to {}",
                    source_path.display(),
                    archive_target.display()
                )
            })?;
        } else {
            fs::remove_file(source_path)
                .with_context(|| format!("failed to remove {}", source_path.display()))?;
        }
    }

    let archived_original_rel_path = archive_target.map(|archive_target| {
        archive_target
            .strip_prefix(roms_dir)
            .unwrap_or(&archive_target)
            .to_path_buf()
    });

    Ok((
        final_rel_path,
        file_name_string(&final_abs_path)?,
        archived_original_rel_path,
    ))
}

fn install_tools(paths: &AppPaths) -> Result<()> {
    fs::create_dir_all(&paths.bin_dir).context("failed to create bin directory")?;
    for download in tool_download_plan(paths)? {
        if download.destination.exists() {
            continue;
        }

        println!("Downloading {}...", download.destination.display());
        if let Some(suffix) = download.zipped_entry_suffix {
            download_and_extract_zip(&download.url, suffix, &download.destination)?;
        } else {
            download_to_path(&download.url, &download.destination)?;
        }

        if download.executable {
            make_executable(&download.destination)?;
        }
    }

    println!("Tool installation complete.");
    Ok(())
}

fn ensure_tools(paths: &AppPaths) -> Result<()> {
    if paths.ctrdecrypt.exists()
        && paths.ctrtool.exists()
        && paths.makerom.exists()
        && paths.seeddb.exists()
    {
        return Ok(());
    }

    install_tools(paths)
}

fn tool_download_plan(paths: &AppPaths) -> Result<Vec<ToolDownload>> {
    let plan = match (env::consts::OS, env::consts::ARCH) {
        ("macos", "aarch64") => vec![
            ToolDownload {
                destination: paths.ctrdecrypt.clone(),
                url: format!(
                    "https://github.com/shijimasoft/ctrdecrypt/releases/download/v{CTRDECRYPT_VER}/ctrdecrypt-macos-universal.zip"
                ),
                zipped_entry_suffix: Some("ctrdecrypt"),
                executable: true,
            },
            ToolDownload {
                destination: paths.ctrtool.clone(),
                url: format!(
                    "https://github.com/3DSGuy/Project_CTR/releases/download/ctrtool-v{CTRTOOL_VER}/ctrtool-v{CTRTOOL_VER}-macos_arm64.zip"
                ),
                zipped_entry_suffix: Some("ctrtool"),
                executable: true,
            },
            ToolDownload {
                destination: paths.makerom.clone(),
                url: format!(
                    "https://github.com/3DSGuy/Project_CTR/releases/download/makerom-v{MAKEROM_VER}/makerom-v{MAKEROM_VER}-macos_arm64.zip"
                ),
                zipped_entry_suffix: Some("makerom"),
                executable: true,
            },
        ],
        ("macos", "x86_64") => vec![
            ToolDownload {
                destination: paths.ctrdecrypt.clone(),
                url: format!(
                    "https://github.com/shijimasoft/ctrdecrypt/releases/download/v{CTRDECRYPT_VER}/ctrdecrypt-macos-universal.zip"
                ),
                zipped_entry_suffix: Some("ctrdecrypt"),
                executable: true,
            },
            ToolDownload {
                destination: paths.ctrtool.clone(),
                url: format!(
                    "https://github.com/3DSGuy/Project_CTR/releases/download/ctrtool-v{CTRTOOL_VER}/ctrtool-v{CTRTOOL_VER}-macos_x86_64.zip"
                ),
                zipped_entry_suffix: Some("ctrtool"),
                executable: true,
            },
            ToolDownload {
                destination: paths.makerom.clone(),
                url: format!(
                    "https://github.com/3DSGuy/Project_CTR/releases/download/makerom-v{MAKEROM_VER}/makerom-v{MAKEROM_VER}-macos_x86_64.zip"
                ),
                zipped_entry_suffix: Some("makerom"),
                executable: true,
            },
        ],
        ("linux", "x86_64") => vec![
            ToolDownload {
                destination: paths.ctrdecrypt.clone(),
                url: format!(
                    "https://github.com/shijimasoft/ctrdecrypt/releases/download/v{CTRDECRYPT_VER}/ctrdecrypt-linux-x86_64.zip"
                ),
                zipped_entry_suffix: Some("ctrdecrypt"),
                executable: true,
            },
            ToolDownload {
                destination: paths.ctrtool.clone(),
                url: format!(
                    "https://github.com/3DSGuy/Project_CTR/releases/download/ctrtool-v{CTRTOOL_VER}/ctrtool-v{CTRTOOL_VER}-ubuntu_x86_64.zip"
                ),
                zipped_entry_suffix: Some("ctrtool"),
                executable: true,
            },
            ToolDownload {
                destination: paths.makerom.clone(),
                url: format!(
                    "https://github.com/3DSGuy/Project_CTR/releases/download/makerom-v{MAKEROM_VER}/makerom-v{MAKEROM_VER}-ubuntu_x86_64.zip"
                ),
                zipped_entry_suffix: Some("makerom"),
                executable: true,
            },
        ],
        (os, arch) => bail!("unsupported platform for tool installation: {os}/{arch}"),
    };

    let mut plan = plan;
    plan.push(ToolDownload {
        destination: paths.seeddb.clone(),
        url: SEEDDB_URL.to_string(),
        zipped_entry_suffix: None,
        executable: false,
    });
    Ok(plan)
}

fn download_to_path(url: &str, destination: &Path) -> Result<()> {
    let response = ureq::get(url)
        .call()
        .with_context(|| format!("failed to download {url}"))?;
    let mut reader = response.into_reader();
    let mut file = File::create(destination)
        .with_context(|| format!("failed to create {}", destination.display()))?;
    io::copy(&mut reader, &mut file)
        .with_context(|| format!("failed to write {}", destination.display()))?;
    Ok(())
}

fn download_and_extract_zip(url: &str, entry_suffix: &str, destination: &Path) -> Result<()> {
    let response = ureq::get(url)
        .call()
        .with_context(|| format!("failed to download {url}"))?;
    let mut bytes = Vec::new();
    response
        .into_reader()
        .read_to_end(&mut bytes)
        .with_context(|| format!("failed to read zip payload from {url}"))?;

    let cursor = io::Cursor::new(bytes);
    let mut zip = ZipArchive::new(cursor).context("failed to open downloaded zip archive")?;

    for index in 0..zip.len() {
        let mut entry = zip.by_index(index).context("failed to inspect zip entry")?;
        let name = entry.name().replace('\\', "/");
        if !name.ends_with(entry_suffix) {
            continue;
        }

        let mut file = File::create(destination)
            .with_context(|| format!("failed to create {}", destination.display()))?;
        io::copy(&mut entry, &mut file)
            .with_context(|| format!("failed to extract {}", destination.display()))?;
        return Ok(());
    }

    bail!("archive from {url} did not contain an entry ending with {entry_suffix}");
}

fn load_state(paths: &AppPaths) -> Result<StateFile> {
    if !paths.state_file.exists() {
        return Ok(StateFile::default());
    }

    let file = File::open(&paths.state_file)
        .with_context(|| format!("failed to open {}", paths.state_file.display()))?;
    let reader = BufReader::new(file);
    let state = serde_json::from_reader(reader)
        .with_context(|| format!("failed to parse {}", paths.state_file.display()))?;
    Ok(state)
}

fn load_config(paths: &AppPaths) -> Result<AppConfig> {
    if !paths.config_file.exists() {
        return Ok(AppConfig::default());
    }

    let file = File::open(&paths.config_file)
        .with_context(|| format!("failed to open {}", paths.config_file.display()))?;
    let reader = BufReader::new(file);
    let config = serde_json::from_reader(reader)
        .with_context(|| format!("failed to parse {}", paths.config_file.display()))?;
    Ok(config)
}

fn save_state(paths: &AppPaths, state: &StateFile) -> Result<()> {
    let tmp_path = paths.state_file.with_extension("json.tmp");
    let file = File::create(&tmp_path)
        .with_context(|| format!("failed to create {}", tmp_path.display()))?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, state)
        .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, &paths.state_file)
        .with_context(|| format!("failed to move {} into place", paths.state_file.display()))?;
    Ok(())
}

fn save_config(paths: &AppPaths, config: &AppConfig) -> Result<()> {
    let tmp_path = paths.config_file.with_extension("json.tmp");
    let file = File::create(&tmp_path)
        .with_context(|| format!("failed to create {}", tmp_path.display()))?;
    let writer = BufWriter::new(file);
    serde_json::to_writer_pretty(writer, config)
        .with_context(|| format!("failed to write {}", tmp_path.display()))?;
    fs::rename(&tmp_path, &paths.config_file)
        .with_context(|| format!("failed to move {} into place", paths.config_file.display()))?;
    Ok(())
}

fn successful_paths_for(state: &StateFile, roms_dir: &Path) -> HashSet<String> {
    let mut paths = HashSet::new();
    let roms_dir = roms_dir.to_string_lossy();

    for record in state.records.iter().filter(|record| {
        record.status == "success"
            && record
                .roms_folder
                .as_deref()
                .map(|folder| folder == roms_dir)
                .unwrap_or(false)
    }) {
        paths.insert(record.source_rel_path.clone());
        if let Some(final_rel_path) = &record.final_rel_path {
            paths.insert(final_rel_path.clone());
        }
    }

    paths
}

fn log_line(paths: &AppPaths, message: String) -> Result<()> {
    fs::create_dir_all(&paths.logs_dir).context("failed to create logs directory")?;
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.sync_log)
        .with_context(|| format!("failed to open {}", paths.sync_log.display()))?;
    writeln!(file, "{} {}", iso_timestamp(), message)
        .with_context(|| format!("failed to write {}", paths.sync_log.display()))?;
    Ok(())
}

fn run_command(program: &Path, args: &[String], cwd: &Path, run_log: &mut File) -> Result<String> {
    writeln!(run_log, "$ {} {}", program.display(), args.join(" "))?;

    let output = Command::new(program)
        .current_dir(cwd)
        .args(args)
        .output()
        .with_context(|| format!("failed to execute {}", program.display()))?;

    run_log.write_all(&output.stdout)?;
    run_log.write_all(&output.stderr)?;

    let mut combined = String::from_utf8_lossy(&output.stdout).into_owned();
    if !output.stderr.is_empty() {
        combined.push_str(&String::from_utf8_lossy(&output.stderr));
    }

    if !output.status.success() {
        bail!(
            "{} exited with status {}",
            program.display(),
            output
                .status
                .code()
                .map(|code| code.to_string())
                .unwrap_or_else(|| "terminated by signal".to_string())
        );
    }

    Ok(combined)
}

fn classify_cia(output: &str) -> Result<CiaKind> {
    let game = Regex::new(r"T.*d.*00040000").unwrap();
    let patch = Regex::new(r"T.*d.*0004000[eE]").unwrap();
    let dlc = Regex::new(r"T.*d.*0004008[cC]").unwrap();

    if game.is_match(output) {
        Ok(CiaKind::Game)
    } else if patch.is_match(output) {
        Ok(CiaKind::Patch)
    } else if dlc.is_match(output) {
        Ok(CiaKind::Dlc)
    } else {
        bail!("unsupported CIA layout")
    }
}

fn collect_ncch_files(stage_dir: &Path, stem: &str) -> Result<Vec<String>> {
    let prefix = format!("{stem}.");
    let mut files = Vec::new();
    for entry in fs::read_dir(stage_dir)
        .with_context(|| format!("failed to read {}", stage_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with(&prefix) && name.ends_with(".ncch") {
            files.push(name);
        }
    }
    if files.is_empty() {
        bail!("no NCCH files were generated for {stem}");
    }
    Ok(files)
}

fn collect_all_ncch(stage_dir: &Path) -> Result<Vec<String>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(stage_dir)
        .with_context(|| format!("failed to read {}", stage_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".ncch") {
            files.push(name);
        }
    }
    if files.is_empty() {
        bail!("no NCCH files were generated");
    }
    Ok(files)
}

fn collect_partitioned_ncch(stage_dir: &Path, stem: &str) -> Result<Vec<(u32, String)>> {
    let prefix = format!("{stem}.");
    let mut files = Vec::new();
    for entry in fs::read_dir(stage_dir)
        .with_context(|| format!("failed to read {}", stage_dir.display()))?
    {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with(&prefix) || !name.ends_with(".ncch") {
            continue;
        }

        let suffix = name
            .strip_prefix(&prefix)
            .and_then(|value| value.strip_suffix(".ncch"))
            .ok_or_else(|| anyhow!("failed to parse partition from {}", name))?;
        let Some(partition_text) = suffix.split('.').next() else {
            continue;
        };
        let Ok(partition) = partition_text.parse::<u32>() else {
            continue;
        };
        files.push((partition, name));
    }
    files.sort_by_key(|(partition, _)| *partition);
    if files.is_empty() {
        bail!("no partitioned NCCH files were generated for {stem}");
    }
    Ok(files)
}

fn ncsd_partition_index(stem: &str, file_name: &str) -> Result<u8> {
    let suffix = file_name
        .strip_prefix(&format!("{stem}."))
        .and_then(|value| value.strip_suffix(".ncch"))
        .ok_or_else(|| anyhow!("failed to parse partition from {}", file_name))?;
    let partition_name = suffix.split('.').next().unwrap_or(suffix);
    match partition_name {
        "Main" => Ok(0),
        "Manual" => Ok(1),
        "DownloadPlay" | "Download Play" => Ok(2),
        "Partition4" => Ok(3),
        "Partition5" => Ok(4),
        "Partition6" => Ok(5),
        "N3DSUpdateData" => Ok(6),
        "UpdateData" => Ok(7),
        _ => bail!("unrecognized NCSD partition in {}", file_name),
    }
}

fn rom_kind_from_path(path: &Path) -> Option<RomKind> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    match extension.as_str() {
        "3ds" => Some(RomKind::ThreeDs),
        "cia" => Some(RomKind::Cia),
        _ => None,
    }
}

fn file_size(path: &Path) -> Result<u64> {
    Ok(fs::metadata(path)
        .with_context(|| format!("failed to stat {}", path.display()))?
        .len())
}

fn file_name_string(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(OsStr::to_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("failed to read file name for {}", path.display()))
}

fn stem_string(path: &Path) -> Result<String> {
    path.file_stem()
        .and_then(OsStr::to_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("failed to read file stem for {}", path.display()))
}

fn sanitize_label(path: &Path) -> String {
    path.to_string_lossy()
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ' ' => '_',
            c if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '(' | ')') => c,
            _ => '_',
        })
        .collect()
}

fn run_log_path_for(paths: &AppPaths, source_rel_path: &Path) -> PathBuf {
    paths
        .run_logs_dir
        .join(format!("{}.log", sanitize_label(source_rel_path)))
}

fn temp_copy_path(path: &Path) -> PathBuf {
    let file_name = path.file_name().and_then(OsStr::to_str).unwrap_or("output");
    path.with_file_name(format!(".{file_name}.tmp"))
}

fn unique_archive_path(originals_root: &Path, source_rel_path: &Path) -> Result<PathBuf> {
    let initial = originals_root.join(source_rel_path);
    if !initial.exists() {
        return Ok(initial);
    }

    let parent = initial
        .parent()
        .ok_or_else(|| anyhow!("{} has no parent", initial.display()))?;
    let stem = initial
        .file_stem()
        .and_then(OsStr::to_str)
        .ok_or_else(|| anyhow!("failed to read file stem for {}", initial.display()))?;
    let extension = initial.extension().and_then(OsStr::to_str);
    let stamp = Utc::now().format("%Y%m%dT%H%M%SZ");
    let new_name = match extension {
        Some(ext) => format!("{stem}-{stamp}.{ext}"),
        None => format!("{stem}-{stamp}"),
    };
    Ok(parent.join(new_name))
}

fn iso_timestamp() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn binary_name(base: &str) -> String {
    if cfg!(windows) {
        format!("{base}.exe")
    } else {
        base.to_string()
    }
}

fn make_executable(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut perms = fs::metadata(path)
            .with_context(|| format!("failed to stat {}", path.display()))?
            .permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms)
            .with_context(|| format!("failed to chmod {}", path.display()))?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn finalize_output_archives_then_replaces_same_extension() -> Result<()> {
        let temp = tempdir()?;
        let roms_dir = temp.path();
        let source_path = roms_dir.join("game.3ds");
        let stage_dir = roms_dir.join("stage");
        fs::create_dir_all(&stage_dir)?;
        fs::write(&source_path, b"original")?;
        let output_stage_path = stage_dir.join("game-decrypted.3ds");
        fs::write(&output_stage_path, b"decrypted")?;

        let (final_rel_path, final_name, archived_original_rel_path) = finalize_output(
            roms_dir,
            &source_path,
            Path::new("game.3ds"),
            &output_stage_path,
            true,
        )?;

        assert_eq!(final_rel_path, PathBuf::from("game.3ds"));
        assert_eq!(final_name, "game.3ds");
        assert_eq!(
            archived_original_rel_path,
            Some(PathBuf::from("originals/game.3ds"))
        );
        assert_eq!(fs::read(&source_path)?, b"decrypted");
        assert_eq!(fs::read(roms_dir.join("originals/game.3ds"))?, b"original");
        Ok(())
    }

    #[test]
    fn finalize_output_archives_after_writing_new_extension() -> Result<()> {
        let temp = tempdir()?;
        let roms_dir = temp.path();
        let source_path = roms_dir.join("game.cia");
        let stage_dir = roms_dir.join("stage");
        fs::create_dir_all(&stage_dir)?;
        fs::write(&source_path, b"original")?;
        let output_stage_path = stage_dir.join("game-decrypted.cci");
        fs::write(&output_stage_path, b"decrypted")?;

        let (final_rel_path, final_name, archived_original_rel_path) = finalize_output(
            roms_dir,
            &source_path,
            Path::new("game.cia"),
            &output_stage_path,
            true,
        )?;

        assert_eq!(final_rel_path, PathBuf::from("game-decrypted.cci"));
        assert_eq!(final_name, "game-decrypted.cci");
        assert_eq!(
            archived_original_rel_path,
            Some(PathBuf::from("originals/game.cia"))
        );
        assert!(!source_path.exists());
        assert_eq!(fs::read(roms_dir.join("game-decrypted.cci"))?, b"decrypted");
        assert_eq!(fs::read(roms_dir.join("originals/game.cia"))?, b"original");
        Ok(())
    }

    #[test]
    fn successful_paths_only_uses_matching_folder_successes() {
        let state = StateFile {
            version: 1,
            records: vec![
                ProcessRecord {
                    status: "success".to_string(),
                    processed_at: "2026-03-26T00:00:00Z".to_string(),
                    roms_folder: Some("/roms/a".to_string()),
                    source_rel_path: "alpha.3ds".to_string(),
                    source_name: "alpha.3ds".to_string(),
                    source_size: 1,
                    output_name: Some("alpha.3ds".to_string()),
                    output_size: Some(1),
                    final_rel_path: Some("alpha.3ds".to_string()),
                    final_name: Some("alpha.3ds".to_string()),
                    archived_original_rel_path: Some("originals/alpha.3ds".to_string()),
                    run_log_path: "logs/run-logs/alpha.log".to_string(),
                    error: None,
                },
                ProcessRecord {
                    status: "failed".to_string(),
                    processed_at: "2026-03-26T00:00:00Z".to_string(),
                    roms_folder: Some("/roms/a".to_string()),
                    source_rel_path: "beta.3ds".to_string(),
                    source_name: "beta.3ds".to_string(),
                    source_size: 1,
                    output_name: None,
                    output_size: None,
                    final_rel_path: None,
                    final_name: None,
                    archived_original_rel_path: None,
                    run_log_path: "logs/run-logs/beta.log".to_string(),
                    error: Some("boom".to_string()),
                },
                ProcessRecord {
                    status: "success".to_string(),
                    processed_at: "2026-03-26T00:00:00Z".to_string(),
                    roms_folder: Some("/roms/b".to_string()),
                    source_rel_path: "gamma.3ds".to_string(),
                    source_name: "gamma.3ds".to_string(),
                    source_size: 1,
                    output_name: Some("gamma.3ds".to_string()),
                    output_size: Some(1),
                    final_rel_path: Some("gamma.3ds".to_string()),
                    final_name: Some("gamma.3ds".to_string()),
                    archived_original_rel_path: Some("originals/gamma.3ds".to_string()),
                    run_log_path: "logs/run-logs/gamma.log".to_string(),
                    error: None,
                },
            ],
        };

        let paths = successful_paths_for(&state, Path::new("/roms/a"));
        assert!(paths.contains("alpha.3ds"));
        assert!(!paths.contains("beta.3ds"));
        assert!(!paths.contains("gamma.3ds"));
    }
}
