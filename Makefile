.PHONY: explore

data/enwiki-latest-pages-articles.xml.bz2:
	mkdir -p data
	cd data && wget enwiki-latest-pages-articles.xml.bz2

data/archive_dump.bincode: data/enwiki-latest-pages-articles.xml.bz2
	cargo run --release -- parse \
		--archive-path data/enwiki-latest-pages-articles.xml.bz2 \
		--dump-path data/archive_dump.bincode

data/wikipedia.rdb: data/archive_dump.bincode
	time cargo run --release -- index \
		--dump-path data/archive_dump.bincode \
		--database-path data/wikipedia.rdb

explore: data/wikipedia.rdb
	cargo run --release -- explore --database-path data/wikipedia.rdb

default: explore
