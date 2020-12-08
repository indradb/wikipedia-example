.PHONY: explorer clean

venv:
	virtualenv -p python3 venv
	. venv/bin/activate && pip install -r requirements.txt

data:
	mkdir data

data/wikipedia.rdb: data
	. venv/bin/activate && ./server.py
	cd inserter && cargo build --release
	./inserter/target/release/indradb-wikipedia-inserter enwiki-latest-pages-articles.xml.bz2 data/archive_dump.bincode
	. venv/bin/activate && ./server.py --stop

explorer: data/wikipedia.rdb venv
	. venv/bin/activate && ./server.py
	. venv/bin/activate && python explorer.py
	. venv/bin/activate && ./server.py --stop

clean:
	rm -rf venv data
