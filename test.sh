#!/bin/sh

set -ev

DB=link_by_comment
njobs=4

cargo test
cargo build --release

P=`pwd`

# create the DB first
rm -fv /NoBackup/${DB}.hll /NoBackup/${DB}-journal.hll
cargo run --release --bin=hyperlogocomplete-populate /NoBackup/${DB}.hll < /dev/null


pv $P/../${DB}.out | (cd ~/sync/src/pipelines-rs && cargo run --release --example=mr_tools -- \
    -M${njobs} -R${njobs} --sort="LOCALE=C gsort --parallel=${njobs} -S1G" \
    -m "(cd $P && cargo run --release --bin=hyperlogocomplete-tokenise)" \
    -r "(cd $P && cargo run --release --bin=hyperlogocomplete-populate /NoBackup/${DB}.hll)"
)

cargo run --release --bin=hyperlogocomplete-query -- /NoBackup/${DB}.hll "superhero comic"

