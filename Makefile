.PHONY: explore init rebuild

init:
	git submodule update --init --recursive
	make data/wikipedia.rdb

data/enwiki-latest-pages-articles.xml.bz2:
	mkdir -p data
	cd data && wget 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2'

rebuild:
	cargo build --release
	cd indradb/server && cargo build --release

data/wikipedia.rdb: data/enwiki-latest-pages-articles.xml.bz2 rebuild
	time target/release/indradb-wikipedia index \
		--archive-path data/enwiki-latest-pages-articles.xml.bz2 \
		--database-path data/wikipedia.rdb

explore: data/wikipedia.rdb rebuild
	target/release/indradb-wikipedia explore --database-path data/wikipedia.rdb

default: explore
