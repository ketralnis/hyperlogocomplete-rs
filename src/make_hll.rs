use std::io;
use std::io::BufRead;

use basichll::HLL;
use clap::{App, Arg};
use itertools::Itertools;

use super::model::HyperLogLogger;
use super::ERROR_RATE;

pub fn main(app_name: &str) {
    let matches = App::new(app_name)
        .arg(
            Arg::with_name("fname")
                .help("the path to the hll db to create")
                .required(true),
        )
        .get_matches();

    let fname = matches.value_of("fname").expect("missing fname");

    let mut model = HyperLogLogger::new(fname).expect("failed to build model");

    let stdin = io::stdin();
    let locked = stdin.lock();
    let entries = locked
        .lines()
        .filter_map(Result::ok)
        .map(|line| {
            let mut splitted = line.splitn(4, "\t");

            let _key = if let Some(x) = optimistic(splitted.next()) {
                x
            } else {
                return None;
            };

            let token = if let Some(x) = optimistic(splitted.next()) {
                x
            } else {
                return None;
            };
            let subreddit = if let Some(x) = optimistic(splitted.next()) {
                x
            } else {
                return None;
            };
            let fullname = if let Some(x) = optimistic(splitted.next()) {
                x
            } else {
                return None;
            };

            return Some(((token, subreddit), fullname));
        })
        .filter_map(|l| l)
        .group_by(|&(ref k, ref _v)| k.clone());

    let prepareds =
        entries.into_iter().map(|((token, subreddit), fullnames)| {
            let fullnames = fullnames.map(|(_k, v)| v);
            let mut hll = HLL::new(ERROR_RATE);
            for fullname in fullnames {
                hll.insert(&fullname);
            }
            let prepared = HyperLogLogger::prepare_hll(token, subreddit, hll);
            prepared
        });

    for group in prepareds.chunks(100).into_iter() {
        let mut transaction = model.transaction();
        for prepared in group {
            transaction.insert(prepared);
        }
        transaction.commit();
    }
}

fn optimistic(text: Option<&str>) -> Option<String> {
    if !text.is_some() {
        return None;
    }
    let text = text.unwrap().to_owned();
    Some(text)
}
