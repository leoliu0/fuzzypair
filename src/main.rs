use anyhow::Context;
use csv::*;
use fuzzywuzzy::fuzz;
use indicatif::ProgressBar;
use num_cpus;
use serde::Serialize;
use std::fs;
use std::path::PathBuf;
use std::sync::mpsc::channel;
use std::sync::Arc;
use structopt::StructOpt;
use threadpool::ThreadPool;

#[derive(Serialize)]
struct Result {
    e1: String,
    e2: String,
    score: u8,
}
enum First {
    V1V2,
    V2V1,
}

// fn score<'a>(threshold: u8, e1: String, v2: Arc<Vec<String>>) -> Vec<Result> {
fn score(threshold: u8, e1: String, v2: Arc<Vec<String>>) -> Vec<Vec<String>> {
    let mut resultvec = Vec::new();
    for e2 in v2.iter() {
        let score = fuzz::token_set_ratio(&e1, &e2, true, true);
        if score >= threshold {
            resultvec.push(vec![e1.to_string(), e2.to_string(), score.to_string()])
            // resultvec.push(Result {
            // e1: e1.to_string(),
            // e2: e2.to_string(),
            // score,
            // })
        }
    }
    resultvec
}

// fn cal(args: &'static Opt, v1: &'static Vec<String>, v2: &'static Vec<String>) {
fn cal(threshold: u8, v1: Vec<String>, v2: Vec<String>, first: First) {
    let v1_len = v1.len();
    let pool = ThreadPool::new(num_cpus::get());
    let (tx, rx) = channel();
    let v2 = Arc::new(v2);
    // let v2 = v2.to_owned();
    for e1 in v1.into_iter() {
        let tx = tx.clone();
        let v2 = Arc::clone(&v2);
        pool.execute(move || tx.send(score(threshold, e1, v2)).expect("lalalala"))
    }
    let mut wtr = WriterBuilder::new().from_path("__result__.csv").unwrap();
    wtr.write_record(["a", "b", "s"]).unwrap();

    let bar = ProgressBar::new(v1_len as u64);
    match first {
        First::V1V2 => {
            for x in rx.iter().take(v1_len) {
                bar.inc(1);
                for y in x.iter() {
                    wtr.write_record(y).unwrap();
                }
            }
        }
        First::V2V1 => {
            for x in rx.iter().take(v1_len) {
                for y in x.iter() {
                    wtr.write_record([&y[1], &y[0], &y[2]]).unwrap();
                    bar.inc(1);
                }
            }
        }
    }
    wtr.flush().unwrap();
}

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(required(true), parse(from_os_str))]
    file1: PathBuf,
    #[structopt(required(true), parse(from_os_str))]
    file2: PathBuf,
    #[structopt(default_value("91"))]
    threshold: u8,
}

fn main() {
    let args: Opt = Opt::from_args();
    let v1: Vec<String> = process_file(&args.file1);
    let v2: Vec<String> = process_file(&args.file2);
    if v1.len() <= v2.len() {
        cal(args.threshold, v1, v2, First::V1V2);
    } else {
        cal(args.threshold, v2, v1, First::V2V1);
    }
}

fn process_file(path: &PathBuf) -> Vec<String> {
    let mut vector = Vec::new();
    let s1 = fs::read_to_string(path)
        .with_context(|| format!("cannot read file {:?}", path))
        .unwrap();
    for line in s1.lines() {
        if !line.trim().is_empty() {
            vector.push(line.to_string());
        }
    }
    vector
}
