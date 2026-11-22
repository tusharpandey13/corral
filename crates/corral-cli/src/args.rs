//! Command line definition.

use clap::{Args, Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "corral", version, about = "Run heavy commands on whichever of your machines has room, without hurting the one you use.")]
pub struct Cli {
    /// Configuration file (default: $CORRAL_CONFIG, the per-user file, then /etc/corral/corral.toml).
    #[arg(long, global = true)]
    pub config: Option<PathBuf>,
    #[arg(long, global = true)]
    pub nats_url: Option<String>,
    #[arg(long, global = true)]
    pub http_url: Option<String>,
    /// Caller identity: owner, agent:<host>.
    #[arg(long, global = true)]
    pub identity: Option<String>,
    #[arg(long, global = true)]
    pub nkey_seed_file: Option<PathBuf>,
    #[command(subcommand)]
    pub cmd: Cmd,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Queue a command to run on a worker.
    Submit(Box<SubmitArgs>),
    /// Follow a job: live logs on stdout/stderr, exit with the job's exit code (125 if it was killed, lost,
    /// cancelled or expired).
    Wait { id: String },
    /// Show a job's logs.
    Logs {
        id: String,
        /// Keep following until the job ends.
        #[arg(short, long)]
        follow: bool,
        /// Also show corral's own notes (`sys` stream).
        #[arg(long)]
        sys: bool,
    },
    /// Job status, including why it waits.
    Status {
        id: Option<String>,
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
    },
    /// Cancel a job.
    Cancel {
        id: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// Submit a finished job again (killed jobs: owner only).
    Resubmit { id: String },
    /// Download a job's artifacts.
    Artifacts {
        id: String,
        #[arg(short, long, default_value = ".")]
        output: PathBuf,
    },
    /// Workers: presence, admission, running jobs.
    Workers {
        #[arg(long)]
        json: bool,
    },
    /// Owner actions on the queue and workers.
    #[command(subcommand)]
    Admin(AdminCmd),
    /// Run the scheduler daemon (on `server`).
    Scheduler,
    /// Run the worker daemon.
    Worker,
    /// Validate a configuration file and print the effective settings.
    #[command(subcommand)]
    Config(ConfigCmd),
    /// Generate an nkey for a NATS identity: writes the seed (mode 0600) and prints the public key.
    Keygen {
        #[arg(long)]
        out: PathBuf,
    },
    /// Print a nats-server configuration for the given identities and public keys.
    NatsConfig {
        #[arg(long, default_value = "127.0.0.1:4222")]
        listen: String,
        #[arg(long, default_value = "/var/lib/nats")]
        store_dir: String,
        /// IDENTITY=PUBLIC_KEY, e.g. scheduler=U... or worker:server=U...; repeatable.
        #[arg(long = "user", required = true)]
        users: Vec<String>,
    },
}

#[derive(Subcommand, Debug)]
pub enum ConfigCmd {
    Check { file: Option<PathBuf> },
}

#[derive(Subcommand, Debug)]
pub enum AdminCmd {
    /// Hold the whole queue, or one worker with --worker.
    Pause {
        #[arg(long)]
        worker: Option<String>,
        #[arg(long)]
        reason: Option<String>,
    },
    Resume {
        #[arg(long)]
        worker: Option<String>,
    },
    /// Stop a worker taking new work; it finishes what it runs.
    Drain { worker: String },
    /// Delete finished jobs past retention now.
    Retention,
}

#[derive(Args, Debug, Default, Clone)]
pub struct SubmitArgs {
    /// Git repository URL or path (use with --ref).
    #[arg(long)]
    pub repo: Option<String>,
    /// Ref to run; resolved to a sha now. With no --repo, the current repository's origin.
    #[arg(long = "ref")]
    pub git_ref: Option<String>,
    /// Snapshot of the current working tree, including uncommitted changes (the default inside a git repository).
    #[arg(long)]
    pub snapshot: bool,
    #[arg(long)]
    pub no_input: bool,
    /// Send files that look like secrets anyway.
    #[arg(long)]
    pub allow_secret_files: bool,
    #[arg(long, default_value_t = 1024)]
    pub snapshot_max_mb: u64,
    /// Required capabilities, comma separated (macos,metal,...).
    #[arg(long, value_delimiter = ',')]
    pub caps: Vec<String>,
    #[arg(long)]
    pub prefer: Option<String>,
    /// How long to wait for the preferred worker before others may take it: 30m, or never.
    #[arg(long)]
    pub fallback_after: Option<String>,
    /// Declared RAM (required for heavy jobs): 8G, 512M.
    #[arg(long)]
    pub ram: Option<String>,
    #[arg(long)]
    pub light: bool,
    #[arg(long)]
    pub exclusive: bool,
    #[arg(long, default_value = "default")]
    pub profile: String,
    #[arg(long)]
    pub lock: Option<String>,
    #[arg(long)]
    pub no_lock: bool,
    #[arg(long, default_value = "2h")]
    pub timeout: String,
    #[arg(long, default_value_t = 50)]
    pub priority: u8,
    /// RFC 3339 time after which the job expires if it has not started.
    #[arg(long)]
    pub not_after: Option<String>,
    /// Paths (relative to the working directory) to bring back; repeatable.
    #[arg(long = "artifacts")]
    pub artifacts: Vec<String>,
    /// Keep results past normal retention.
    #[arg(long)]
    pub keep: bool,
    #[arg(long, default_value = "on_fail")]
    pub notify: String,
    #[arg(long, value_delimiter = ',')]
    pub pause_on_exit: Vec<i32>,
    #[arg(long = "label")]
    pub labels: Vec<String>,
    /// Environment for the job (nothing else is inherited): K=V; repeatable.
    #[arg(long = "env")]
    pub env: Vec<String>,
    /// Working directory inside the input.
    #[arg(long, default_value = ".")]
    pub cwd: String,
    /// Follow the job after submitting.
    #[arg(long)]
    pub wait: bool,
    #[arg(long)]
    pub json: bool,
    /// The command: one argument is a shell string (run with bash -lc), several are an argv.
    #[arg(last = true, required = true)]
    pub cmd: Vec<String>,
}
