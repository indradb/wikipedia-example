explorer: venv data
	. venv/bin/activate && ./server.py
	. venv/bin/activate && python explorer.py
	. venv/bin/activate && ./server.py --stop

venv:
	virtualenv -p python3 venv
	. venv/bin/activate && pip install -r requirements.txt

data: venv
	mkdir -p data
	cargo build --release
	cargo run --bin crawler --release -- enwiki-latest-pages-articles.xml.bz2 data/archive_dump.bincode

clean:
	rm -rf venv data
