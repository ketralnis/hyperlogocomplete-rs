use std::io;
use std::path::Path;
use std::io::BufRead;

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
        let workers = 8;

        pipelines::Pipeline::from(PbIter::new(lines.into_iter()))
            .ppipe(
                workers,
                |tx: pipelines::Sender<((String, String), String)>,
                 rx: pipelines::LockedReceiver<String>| {
                    for line in rx {
                        let mut splitted = line.split('\t');
                        let subreddit = splitted.next().expect("no subreddit");
                        let fullname = splitted.next().expect("no fullname");
                        let text = splitted.next().expect("no text");
                        let tokens = tokenise(&text);
                        for token in tokens {
                            tx.send((
                                (token, subreddit.to_owned()),
                                fullname.to_owned(),
                            ));
                        }
                    }
                },
            )
            .preduce(workers, |(token, subreddit), fullnames| {
                //println!("starting worker for {:?} ({} entries)", (&token, &subreddit), fullnames.len());
                let count = fullnames.len();
                if count < 3 {
                    return None;
                }
                let mut hll = HLL::new(ERROR_RATE);
                for fullname in fullnames {
                    hll.insert(&fullname);
                }
                Some(HyperLogLogger::prepare_hll(token, subreddit, hll))
            })
            .pipe(move |tx, rx| {
                let mut transaction = model.transaction();

                for prepared in rx {
                    if let Some(prepared) = prepared {
                        transaction.insert(prepared);
                    }
                }
                transaction.commit();
                tx.send(());
            })
            .drain()
    });
}
