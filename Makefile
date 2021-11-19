.PHONY: explore init

init:
	git submodule update --init --recursive
	make data/wikipedia.rdb

indradb/target/release/indradb-server:
	cd indradb/server && cargo build --release

data/enwiki-latest-pages-articles.xml.bz2:
	mkdir -p data
	cd data && wget 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2'

data/wikipedia.rdb: data/enwiki-latest-pages-articles.xml.bz2 indradb/target/release/indradb-server
	time cargo run --release -- index \
		--archive-path data/enwiki-latest-pages-articles.xml.bz2 \
		--database-path data/wikipedia.rdb

explore: data/wikipedia.rdb indradb/target/release/indradb-server
	cargo run --release -- explore --database-path data/wikipedia.rdb

default: explore
