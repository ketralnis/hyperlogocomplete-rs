use std::path::Path;
use std::fs::remove_file;
use std::io::{self, Read, Write};
use std::collections::HashMap;
use std::io::Cursor;

use basichll::HLL;
use flate2::Compression;
use flate2::write::GzEncoder;
use rusqlite;
use flate2::read::GzDecoder;

use super::token::tokenise;

pub struct HyperLogLogger {
    conn: rusqlite::Connection,
}

pub struct Prepared {
    blob: Vec<u8>,
    size: f64,
    subreddit: String,
    token: String,
}

impl Prepared {
    fn new(token: String, subreddit: String, inp: HLL) -> Prepared {
        let size = inp.count();
        let as_vec = inp.into_vec();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder
            .write_all(&as_vec)
            .expect("failed compression write");
        let compressed = encoder.finish().expect("failed compression finish");
        Prepared {
            blob: compressed,
            subreddit,
            token,
            size,
        }
    }
}

impl HyperLogLogger {
    pub fn new<P: AsRef<Path>>(
        fname: P,
    ) -> Result<HyperLogLogger, rusqlite::Error> {
        let conn = rusqlite::Connection::open(fname)?;
        conn.execute_batch(
            "
            PRAGMA journal_mode=NONE;
            PRAGMA synchronous=off;

            CREATE TABLE IF NOT EXISTS
            words(
                token BLOB NOT NULL,
                subreddit BLOB NOT NULL,
                card FLOAT NOT NULL,
                data BLOB NOT NULL,
                PRIMARY KEY (token, subreddit));
            ",
        )?;

        let ret = HyperLogLogger { conn: conn };
        Ok(ret)
    }

    pub fn query(
        &mut self,
        sentence: &str,
        limit: usize,
    ) -> Vec<(f64, String)> {
        let mut my_hlls = HashMap::new();
        for token in tokenise(sentence) {
            for (subreddit, hll) in self.get_hlls(&token) {
                my_hlls
                    .entry(subreddit)
                    .and_modify(|existing| {
                        let new = &*existing + &hll;
                        *existing = new;
                    })
                    .or_insert(hll);
            }
        }
        let mut counted: Vec<(f64, String)> = my_hlls
            .iter()
            .map(|(sr, hll)| (hll.count(), sr.to_owned()))
            .collect();
        counted.sort_by(|a, b| b.partial_cmp(a).expect("got a nan"));
        counted.truncate(limit);
        counted
    }

    fn get_hlls(&mut self, token: &str) -> Vec<(String, HLL)> {
        let mut stmt = self.conn
            .prepare("SELECT subreddit, data FROM words WHERE token=?")
            .expect("failed to prepare");
        let rows = stmt.query_map(&[&token], |row| {
            let sr: String = row.get(0);
            let blob: Vec<u8> = row.get(1);

            let cursor = Cursor::new(&blob);
            let mut decompressed = Vec::new();
            let mut decoder = GzDecoder::new(cursor);
            decoder
                .read_to_end(&mut decompressed)
                .expect("failed to read to end");

            (sr, HLL::from_vec(decompressed))
        }).expect("failed to query map");

        let mut ret = Vec::new();
        for row in rows {
            let (sr, hll) = row.expect("bad row");
            ret.push((sr, hll))
        }
        ret
    }

    pub fn delete_if_exists(fname: &Path) -> Result<(), io::Error> {
        if fname.exists() {
            remove_file(fname)?;
        }
        let journal = fname.with_extension("hll-journal");
        if journal.exists() {
            remove_file(journal)?;
        }
        Ok(())
    }

    pub fn prepare_hll(token: String, subreddit: String, hll: HLL) -> Prepared {
        Prepared::new(token, subreddit, hll)
    }

    pub fn transaction(&mut self) -> Transaction {
        let tx = self.conn.transaction().expect("couldn't start transaction");
        Transaction { tx }
    }
}

pub struct Transaction<'a> {
    tx: rusqlite::Transaction<'a>,
}

impl<'a> Transaction<'a> {
    pub fn insert(&mut self, prepared: Prepared) {
        let success = self.tx.execute(
            "
                INSERT INTO words(token, subreddit, card, data)
                VALUES (?,?,?,?)
                ",
            &[
                &prepared.token,
                &prepared.subreddit,
                &prepared.size,
                &prepared.blob,
            ],
        );
        if !success.is_ok() {
            println!("failed insert: {:?}", success);
        }
    }

    pub fn commit(self) {
        self.tx.commit().expect("failed commit")
    }
}
