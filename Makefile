.PHONY: explore init rebuild

init:
	git submodule update --init --recursive
	make data/wikipedia.rdb

data/enwiki-latest-pages-articles.xml.bz2:
	mkdir -p data
	cd data && wget 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2'

rebuild:
	cargo build --release
	cd indradb && cargo build --release

data/wikipedia.rdb: data/enwiki-latest-pages-articles.xml.bz2 rebuild
	target/release/indradb-wikipedia --database-path data/wikipedia.rdb index \
		--archive-path data/enwiki-latest-pages-articles.xml.bz2
	target/release/indradb-wikipedia --database-path data/wikipedia.rdb analyze

explore: data/wikipedia.rdb rebuild
	target/release/indradb-wikipedia --database-path data/wikipedia.rdb explore

default: explore
