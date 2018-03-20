use std::collections::HashSet;
use std::io::BufRead;
use std::io;

use regex::Regex;
use rust_stemmers::{Algorithm, Stemmer};

pub fn main(_app_name: &str) {
    let stdin = io::stdin();
    let locked = stdin.lock();
    for line in locked.lines() {
        if !line.is_ok() {
            continue;
        }
        let line = line.unwrap();

        let mut splitted = line.splitn(3, "\t");

        let subreddit = if let Some(x) = optimistic(splitted.next()) {
            x
        } else {
            continue;
        };
        let fullname = if let Some(x) = optimistic(splitted.next()) {
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
            println!(
                "{token}/{subreddit}\t{token}\t{subreddit}\t{fullname}",
                token = token,
                subreddit = subreddit,
                fullname = fullname
            );
        }
    }
}

fn optimistic(text: Option<&str>) -> Option<String> {
    if !text.is_some() {
        return None;
    }
    let text = text.unwrap().to_owned();
    Some(text)
}

pub fn tokenise<'a>(sentence: &'a str) -> HashSet<String> {
    lazy_static! {
        static ref RE: Regex = Regex::new("[A-Za-z0-9_-]{3,}").unwrap();
    }
    let en_stemmer = Stemmer::create(Algorithm::English);
    let mut ret = HashSet::new();
    for word in RE.find_iter(sentence) {
        let word = word.as_str();
        if word.len() >= 3 && word.len() < 10 {
            let word = word.to_lowercase();
            let stemmed: String = en_stemmer.stem(&word).to_string();
            ret.insert(stemmed);
        }
    }
    return ret;
}

#[cfg(test)]
mod tests {
    use super::tokenise;

    #[test]
    fn it_works() {
        let mut expected = vec!["asdfasdf", "think", "hello", "there", "here"];
        expected.sort();
        let mut got: Vec<String> = tokenise(
            "hello there I am here'asdfasdf thinking",
        ).into_iter()
            .collect();
        got.sort();
        assert_eq!(got, expected);
    }
}
