use std::collections::HashSet;

use regex::Regex;
use rust_stemmers::{Algorithm, Stemmer};

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
