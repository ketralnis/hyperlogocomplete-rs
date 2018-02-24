#![feature(generators, generator_trait)]
#![feature(conservative_impl_trait, universal_impl_trait)]
#![feature(rustc_private)]

extern crate basichll;
extern crate clap;
extern crate flate2;
extern crate itertools;
#[macro_use]
extern crate lazy_static;
extern crate pbr;
extern crate regex;
extern crate rusqlite;
extern crate rust_stemmers;

pub mod mapreduce;
pub mod token;
pub mod model;

pub const ERROR_RATE: f64 = 0.10;

pub mod make_hll {
    use std::io;
    use std::path::Path;
    use std::io::BufRead;

    use basichll::HLL;
    use clap::{App, Arg};
    use pbr::PbIter;

    use super::model::{HyperLogLogger, Prepared};
    use super::ERROR_RATE;
    use super::token::tokenise;
    use super::utils::timeit;
    use super::mapreduce::mapreduce;

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

        let mut model =
            HyperLogLogger::new(fname).expect("failed to build model");

        let lines: Vec<String> = timeit("reading file", || {
            let mut ret = Vec::new();
            let stdin = io::stdin();
            for line in stdin.lock().lines() {
                ret.push(line.expect("bad line").to_owned())
            }
            ret
        });

        let tuples = timeit("tokenising everything", move || {
            let mut tuples = Vec::new();
            for line in PbIter::new(lines.into_iter()) {
                let mut splitted = line.split('\t');
                let subreddit =
                    splitted.next().expect("no subreddit").to_owned();
                let fullname = splitted.next().expect("no fullname").to_owned();
                let text = splitted.next().expect("no text").to_owned();
                let tokens: Vec<String> = tokenise(&text).into_iter().collect();
                tuples.push((subreddit, fullname, tokens));
            }
            return tuples;
        });

        let hlls: Vec<(String, String, HLL)> =
            timeit("building hlls", move || {
                mapreduce(
                    PbIter::new(tuples.into_iter()),
                    |(subreddit, fullname, tokens)| {
                        let mut ret = Vec::new();
                        for token in tokens {
                            ret.push((
                                (token, subreddit.to_owned()),
                                fullname.to_owned(),
                            ));
                        }
                        ret
                    },
                    |&(ref token, ref subreddit), vals| {
                        let mut hll = HLL::new(ERROR_RATE);
                        for fullname in vals {
                            hll.insert(&fullname);
                        }
                        vec![(token.to_owned(), subreddit.to_owned(), hll)]
                    },
                )
            });

        let prepared: Vec<Prepared> = timeit("preparing hlls", move || {
            let mut prepared = Vec::new();
            for (token, subreddit, hll) in PbIter::new(hlls.into_iter()) {
                let item = HyperLogLogger::prepare_hll(token, subreddit, hll);
                prepared.push(item);
            }
            prepared
        });

        timeit("storing hlls", move || {
            let mut transaction = model.transaction();

            for prepared in PbIter::new(prepared.into_iter()) {
                transaction.insert(prepared);
            }
            transaction.commit()
        });
    }
}

pub mod query {
    pub fn main(_app_name: &str) {}
}

pub mod utils {
    use std::time::Instant;

    pub fn timeit<Out, F: FnOnce() -> Out>(name: &str, func: F) -> Out {
        let start_time = Instant::now();
        println!("starting {}", name);
        let ret = func();
        let duration = start_time.elapsed();
        let took =
            duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;
        println!("finished {} in {:.2}s", name, took);
        ret
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
