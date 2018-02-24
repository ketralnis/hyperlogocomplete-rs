use std::path::Path;
use std::fs::remove_file;
use std::io::{self, Write};

use basichll::HLL;
use flate2::Compression;
use flate2::write::GzEncoder;
use rusqlite;

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
                token NOT NULL,
                subreddit NOT NULL,
                card FLOAT NOT NULL,
                blob NOT NULL,
                PRIMARY KEY (token, subreddit));
            ",
        )?;

        let ret = HyperLogLogger { conn: conn };
        Ok(ret)
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
        self.tx
            .execute(
                "
                INSERT INTO words(token, subreddit, card, blob)
                VALUES (?,?,?,?)
                ",
                &[
                    &prepared.token,
                    &prepared.subreddit,
                    &prepared.size,
                    &prepared.blob,
                ],
            )
            .expect("failed conn execute");
    }

    pub fn commit(self) {
        self.tx.commit().expect("failed commit")
    }
}
