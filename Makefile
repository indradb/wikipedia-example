.PHONY: explore init

init:
	git submodule update --init --recursive
	make data/wikipedia.rdb

indradb/target/release/indradb-server:
	cd indradb/server && cargo build --release

data/enwiki-latest-pages-articles.xml.bz2:
	mkdir -p data
	cd data && wget 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2'

data/archive_dump.bincode: data/enwiki-latest-pages-articles.xml.bz2
	time cargo run --release -- parse \
		--archive-path data/enwiki-latest-pages-articles.xml.bz2 \
		--dump-path data/archive_dump.bincode

data/wikipedia.rdb: data/archive_dump.bincode indradb/target/release/indradb-server
	time cargo run --release -- index \
		--dump-path data/archive_dump.bincode \
		--database-path data/wikipedia.rdb

explore: data/wikipedia.rdb indradb/target/release/indradb-server
	cargo run --release -- explore --database-path data/wikipedia.rdb

default: explore
