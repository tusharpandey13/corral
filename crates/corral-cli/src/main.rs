use clap::Parser;
use corral_cli::args::*;
use corral_cli::client::Client;
use corral_cli::exitcode;
use corral_cli::follow::{FollowOpts, StdSink, exit_code_for, final_line, follow};
use corral_cli::{daemon, render, submit};
use corral_proto::api::AdminReq;
use corral_proto::bus::read_seed;
use corral_proto::config::{ClientCfg, Config};
use corral_proto::envelope::ApiError;
use corral_proto::ident::Identity;
use std::path::PathBuf;

fn now_ms() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn load_config(path: &Option<PathBuf>) -> Result<(Config, Option<PathBuf>), String> {
    match path {
        Some(p) => Ok((Config::load(p).map_err(|e| e.to_string())?, Some(p.clone()))),
        None => {
            let l = Config::load_default().map_err(|e| e.to_string())?;
            Ok((l.config, l.path))
        }
    }
}

async fn connect(cli: &Cli, cfg: &Config) -> Result<Client, ApiError> {
    let mut r = ClientCfg::resolve(cfg.client.as_ref(), &|k| std::env::var(k).ok());
    if let Some(u) = &cli.nats_url {
        r.nats_url = u.clone();
    }
    if let Some(u) = &cli.http_url {
        r.http_url = Some(u.clone());
    }
    if let Some(i) = &cli.identity {
        r.identity = i.clone();
    }
    if let Some(p) = &cli.nkey_seed_file {
        r.nkey_seed_file = Some(p.clone());
    }
    let identity = Identity::parse(&r.identity).ok_or_else(|| ApiError::bad(format!("bad identity {:?}", r.identity)))?;
    let seed = r.nkey_seed_file.as_ref().map(|p| read_seed(p)).transpose().map_err(ApiError::bad)?;
    Client::connect(&r.nats_url, identity, seed, r.http_url).await
}

fn fail(e: &ApiError) -> i32 {
    eprintln!("corral: {}", e.message);
    if let Some(w) = e.extra.get("waiting") {
        eprintln!("  {w}");
    }
    exitcode::for_error(e)
}

fn init_logging() {
    let filter = tracing_subscriber::EnvFilter::try_from_env("CORRAL_LOG").unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let b = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    if std::env::var("CORRAL_LOG_FORMAT").is_ok_and(|v| v == "json") {
        b.json().init();
    } else {
        b.compact().init();
    }
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let code = run(cli).await;
    std::process::exit(code);
}

async fn run(cli: Cli) -> i32 {
    let (cfg, cfg_path) = match load_config(&cli.config) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("corral: {e}");
            return exitcode::USAGE;
        }
    };
    match &cli.cmd {
        Cmd::Scheduler => {
            init_logging();
            return match daemon::run_scheduler(&cfg).await {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("corral scheduler: {e}");
                    1
                }
            };
        }
        Cmd::Worker => {
            init_logging();
            return match daemon::run_worker(&cfg).await {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("corral worker: {e}");
                    1
                }
            };
        }
        Cmd::Config(ConfigCmd::Check { file }) => {
            let target = file.clone().or(cfg_path);
            return match target {
                None => {
                    eprintln!("corral: no configuration file found (set --config or CORRAL_CONFIG)");
                    exitcode::NOT_FOUND
                }
                Some(p) => match Config::load(&p) {
                    Ok(c) => {
                        println!("ok: {}", p.display());
                        let r = ClientCfg::resolve(c.client.as_ref(), &|k| std::env::var(k).ok());
                        for (k, src) in &r.sources {
                            println!("  client.{k}: from {src}");
                        }
                        println!("{c:#?}");
                        0
                    }
                    Err(e) => {
                        eprintln!("{e}");
                        exitcode::USAGE
                    }
                },
            };
        }
        Cmd::Keygen { out } => {
            let kp = nkeys::KeyPair::new_user();
            let seed = match kp.seed() {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("corral: {e}");
                    return 1;
                }
            };
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            match std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o600)
                .open(out)
                .and_then(|mut f| f.write_all(seed.as_bytes()))
            {
                Ok(()) => {
                    println!("{}", kp.public_key());
                    return 0;
                }
                Err(e) => {
                    eprintln!("corral: cannot write {}: {e}", out.display());
                    return 1;
                }
            }
        }
        Cmd::NatsConfig { listen, store_dir, users } => {
            let mut parsed = Vec::new();
            for u in users {
                match u.split_once('=').and_then(|(i, k)| Identity::parse(i).map(|id| (id, k.to_string()))) {
                    Some(p) => parsed.push(p),
                    None => {
                        eprintln!("corral: --user wants IDENTITY=PUBLIC_KEY (scheduler, owner, worker:<host>, agent:<host>), got {u:?}");
                        return exitcode::USAGE;
                    }
                }
            }
            print!("{}", corral_proto::perms::nats_server_conf(listen, store_dir, &parsed));
            return 0;
        }
        _ => {}
    }

    let client = match connect(&cli, &cfg).await {
        Ok(c) => c,
        Err(e) => return fail(&e),
    };
    match cli.cmd {
        Cmd::Submit(a) => submit_cmd(&client, a).await,
        Cmd::Wait { id } => match follow(&client, &id, FollowOpts { follow: true, sys: false }, &mut StdSink).await {
            Ok(j) => {
                if let Some(l) = final_line(&j) {
                    eprintln!("{l}");
                }
                exit_code_for(&j)
            }
            Err(e) => fail(&e),
        },
        Cmd::Logs { id, follow: f, sys } => match follow(&client, &id, FollowOpts { follow: f, sys }, &mut StdSink).await {
            Ok(_) => 0,
            Err(e) => fail(&e),
        },
        Cmd::Status { id, all, json } => match id {
            Some(id) => match client.get(&id).await {
                Ok(j) => {
                    if json {
                        println!("{}", serde_json::to_string_pretty(&j).expect("json"));
                    } else {
                        print!("{}", render::job_detail(&j, now_ms()));
                    }
                    0
                }
                Err(e) => fail(&e),
            },
            None => match client.list(all).await {
                Ok(js) => {
                    if json {
                        println!("{}", serde_json::to_string_pretty(&js).expect("json"));
                    } else if js.is_empty() {
                        println!("no jobs");
                    } else {
                        for j in &js {
                            print!("{}", render::job_row(j, now_ms()));
                            for w in &j.waiting {
                                println!("    {}: {}", w.worker, w.text);
                            }
                        }
                    }
                    0
                }
                Err(e) => fail(&e),
            },
        },
        Cmd::Cancel { id, reason } => match client.cancel(&id, reason).await {
            Ok(r) => {
                println!("{} {}", r.id, if r.cancel_requested { "cancel requested" } else { &r.state });
                0
            }
            Err(e) => fail(&e),
        },
        Cmd::Resubmit { id } => match client.resubmit(&id).await {
            Ok(r) => {
                println!("{}", r.id);
                0
            }
            Err(e) => fail(&e),
        },
        Cmd::Artifacts { id, output } => match client.artifacts(&id, &output).await {
            Ok(_) => {
                println!("artifacts of {id} unpacked into {}", output.display());
                0
            }
            Err(e) => fail(&e),
        },
        Cmd::Workers { json } => match client.workers().await {
            Ok(w) => {
                if json {
                    println!("{}", serde_json::to_string_pretty(&w).expect("json"));
                } else {
                    if w.queue_paused {
                        println!("queue is paused");
                    }
                    if w.workers.is_empty() {
                        println!("no workers have connected");
                    }
                    for row in &w.workers {
                        print!("{}", render::worker_row(row));
                    }
                }
                0
            }
            Err(e) => fail(&e),
        },
        Cmd::Admin(a) => {
            let (verb, req) = match a {
                AdminCmd::Pause { worker, reason } => ("pause", AdminReq { worker, reason }),
                AdminCmd::Resume { worker } => ("resume", AdminReq { worker, reason: None }),
                AdminCmd::Drain { worker } => ("drain", AdminReq { worker: Some(worker), reason: None }),
                AdminCmd::Retention => ("retention", AdminReq::default()),
            };
            match client.admin(verb, &req).await {
                Ok(r) => {
                    println!("{}", r.detail.unwrap_or_else(|| "ok".into()));
                    0
                }
                Err(e) => fail(&e),
            }
        }
        _ => unreachable!("handled above"),
    }
}

async fn submit_cmd(client: &Client, a: SubmitArgs) -> i32 {
    let cwd = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let mut spec = match submit::spec_from_args(&a) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("corral: {e}");
            return exitcode::USAGE;
        }
    };
    let scratch = std::env::temp_dir().join("corral-snapshots");
    let plan = match submit::plan_input(&a, &cwd, &scratch) {
        Ok(p) => p,
        Err(e) => {
            eprintln!("corral: {e}");
            return exitcode::REFUSED;
        }
    };
    spec.input = plan.to_input();
    let snap = match &plan {
        submit::InputPlan::Snapshot(s) => {
            eprintln!("corral: snapshot of {} files, {} bytes", s.files, s.bytes);
            Some(s.as_ref())
        }
        _ => None,
    };
    let reply = client.submit(&spec, snap).await;
    if let Some(s) = snap {
        let _ = std::fs::remove_file(&s.path);
    }
    let reply = match reply {
        Ok(r) => r,
        Err(e) => return fail(&e),
    };
    if a.json {
        println!("{}", serde_json::to_string(&reply).expect("json"));
    } else {
        println!("{}", reply.id);
    }
    if a.wait {
        return match follow(client, &reply.id, FollowOpts { follow: true, sys: false }, &mut StdSink).await {
            Ok(j) => {
                if let Some(l) = final_line(&j) {
                    eprintln!("{l}");
                }
                exit_code_for(&j)
            }
            Err(e) => fail(&e),
        };
    }
    0
}
