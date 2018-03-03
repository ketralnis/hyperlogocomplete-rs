use std::collections::HashMap;
use std::io::Read;
use std::io;
use std::path::Path;

use basichll::HLL;
use clap::{App, Arg};
use pipelines;

use super::model::HyperLogLogger;
use super::ERROR_RATE;
use super::token::tokenise;
use super::utils::timeit;
use super::utils::MyBar;

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

    let lines: Vec<Vec<u8>> = timeit("reading file", || {
        // we could do the splitting in the producer thread instead and it would actually be faster
        // because we could avoid a lot of the copies. but we do it here in the main thread so that
        // we can have an accurate progress bar
        let mut stdin_data = Vec::new();
        let stdin = io::stdin();
        let mut locked = stdin.lock();
        locked.read_to_end(&mut stdin_data).expect("failed read");
        let lines = stdin_data
            .split(|ch| *ch == b'\n')
            .map(|line| line.to_owned())
            .collect();
        lines
    });

    let num_lines = lines.len();

    timeit("building hlls", move || {
        let workers = 4;

        pipelines::Pipeline::from(MyBar::new(
            lines.into_iter(),
            num_lines as u64,
            1000,
        )).configure(
            pipelines::PipelineConfig::default()
                .batch_size(1000)
                .buff_size(5),
        )
            .ppipe(workers, |tx, rx| {
                for line in rx {
                    let mut splitted = line.splitn(3, |ch| *ch == b'\t');

                    let subreddit = if let Some(x) = optimistic(splitted.next())
                    {
                        x
                    } else {
                        continue;
                    };
                    let fullname = if let Some(x) = optimistic(splitted.next())
                    {
                        x
                    } else {
                        continue;
                    };
                    let text = if let Some(x) = optimistic(splitted.next()) {
                        x
                    } else {
                        continue;
                    };

                    let tokens = tokenise(&text);
                    for token in tokens {
                        tx.send((
                            (token, subreddit.to_owned()),
                            fullname.to_owned(),
                        ));
                    }
                }
            })
            .distribute(workers, |tx, rx| {
                // read in all of the data and build HLLs out of them as their data comes in
                let mut hm = HashMap::new();
                for ((token, subreddit), fullname) in rx {
                    hm.entry((token, subreddit))
                        .or_insert_with(|| HLL::new(ERROR_RATE))
                        .insert(&fullname);
                }

                // now we have everything, so prepare the HLLs and send them on to be written
                timeit("preparing hlls", || {
                    let len = { hm.len() as u64 };
                    for ((token, subreddit), hll) in
                        MyBar::new(hm.into_iter(), len, 1000)
                    {
                        if hll.count() < 3.0 {
                            continue;
                        }
                        let prepared =
                            HyperLogLogger::prepare_hll(token, subreddit, hll);
                        tx.send(prepared);
                    }
                });
            })
            .pipe(move |tx, rx| {
                let mut transaction = model.transaction();

                for prepared in rx {
                    transaction.insert(prepared);
                }
                transaction.commit();
                tx.send(()); // signal completion
            })
            .drain()
    });
}

fn optimistic(text: Option<&[u8]>) -> Option<String> {
    if !text.is_some() {
        return None;
    }
    let text = text.unwrap().to_owned();

    let text = String::from_utf8(text);
    if !text.is_ok() {
        return None;
    }
    return Some(text.unwrap());
}
