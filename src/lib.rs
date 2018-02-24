#![feature(generators, generator_trait)]
#![feature(conservative_impl_trait, universal_impl_trait)]
#![feature(rustc_private)]
#![feature(entry_and_modify)]

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
pub mod make_hll;

pub const ERROR_RATE: f64 = 0.10;

pub mod query {
    use std::path::Path;

    use clap::{App, Arg};

    use super::model::HyperLogLogger;

    pub fn main(app_name: &str) {
        let matches = App::new(app_name)
            .arg(
                Arg::with_name("fname")
                    .help("the path to the hll db to create")
                    .required(true),
            )
            .arg(
                Arg::with_name("sentence")
                    .help("the query text")
                    .required(true),
            )
            .get_matches();

        let fname = matches.value_of("fname").expect("missing fname");
        let path = Path::new(fname);
        if !path.exists() {
            panic!("fname doesn't exist");
        }

        let sentence = matches.value_of("sentence").expect("missing sentence");

        let mut model =
            HyperLogLogger::new(fname).expect("failed to build model");

        for (count, subreddit) in model.query(sentence, 10) {
            println!("{:6.3}\t{}", count, subreddit);
        }
    }
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
