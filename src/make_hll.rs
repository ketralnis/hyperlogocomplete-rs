use std::io;
use std::path::Path;
use std::io::BufRead;
use std::sync::mpsc;

use basichll::HLL;
use clap::{App, Arg};
use pbr::PbIter;
use pipelines;

use super::model::HyperLogLogger;
use super::ERROR_RATE;
use super::token::tokenise;
use super::utils::timeit;

pub fn main(app_name: &str) {
    timeit("doing everything", || _main(app_name))
}

pub fn _main(app_name: &str) {
    let matches = App::new(app_name)
        .arg(
            Arg::with_name("fname")
                .help("the path to the hll db to create")
                .required(true),
        )
        .get_matches();

    let fname = matches.value_of("fname").expect("missing fname");
    let path = Path::new(fname);

    HyperLogLogger::delete_if_exists(path)
        .expect("failed to delete preexisting db");

    let mut model = HyperLogLogger::new(fname).expect("failed to build model");

    let lines: Vec<String> = timeit("reading file", || {
        let mut ret = Vec::new();
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            ret.push(line.expect("bad line").to_owned())
        }
        ret
    });

    timeit("building hlls", move || {
        let bufsize = 10; // TODO can we get rid of this noise?
        let workers = 4;

        pipelines::Pipeline::from(PbIter::new(lines.into_iter()), bufsize)
            .then(
                pipelines::Multiplex::from(TokeniserEntry, workers, bufsize),
                bufsize,
            )
            .reduce(
                |(token, subreddit), fullnames| {
                    let mut hll = HLL::new(ERROR_RATE);
                    for fullname in fullnames {
                        hll.insert(&fullname);
                    }
                    HyperLogLogger::prepare_hll(token, subreddit, hll)
                },
                bufsize,
            )
            .pipe(
                move |rx, tx| {
                    let mut transaction = model.transaction();

                    for prepared in rx {
                        transaction.insert(prepared);
                    }
                    transaction.commit();
                    tx.send(()).expect("failed finish");
                },
                bufsize,
            )
            .drain()
    });
}

#[derive(Copy, Clone)]
struct TokeniserEntry;

impl pipelines::PipelineEntry<String, ((String, String), String)>
    for TokeniserEntry
{
    fn process<I: IntoIterator<Item = String>>(
        self,
        rx: I,
        tx: mpsc::SyncSender<((String, String), String)>,
    ) {
        for line in rx {
            let mut splitted = line.split('\t');
            let subreddit = splitted.next().expect("no subreddit").to_string();
            let fullname = splitted.next().expect("no fullname").to_string();
            let text = splitted.next().expect("no text");
            let tokens = tokenise(&text);
            for token in tokens {
                tx.send(((subreddit.to_owned(), token), fullname.to_owned()))
                    .expect("failed self");
            }
        }
    }
}
