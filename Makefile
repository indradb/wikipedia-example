.PHONY: explorer clean

explorer: data
	cargo run --bin explorer --release

data:
	mkdir -p data
	cargo build --release
	time cargo run --bin crawler --release -- enwiki-latest-pages-articles.xml.bz2 data/archive_dump.bincode

clean:
	rm -rf venv data
