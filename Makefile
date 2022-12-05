.PHONY: explore init rebuild

init:
	make data/wikipedia.rdb

data/enwiki-latest-pages-articles.xml.bz2:
	mkdir -p data
	cd data && wget 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2'

rebuild:
	cargo build --release
	cd indradb && cargo build --release

data/wikipedia.rdb: data/enwiki-latest-pages-articles.xml.bz2
	make rebuild
	target/release/indradb-wikipedia --database-path data/wikipedia.rdb index \
		--archive-path data/enwiki-latest-pages-articles.xml.bz2

explore: data/wikipedia.rdb
	make rebuild
	target/release/indradb-wikipedia --database-path data/wikipedia.rdb explore

default: explore
