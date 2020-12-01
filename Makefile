.PHONY: explorer clean

venv:
	virtualenv -p python3 venv
	. venv/bin/activate && pip install -r requirements.txt

data:
	mkdir data

data/links.txt: data venv
	. venv/bin/activate && python extract.py enwiki-latest-pages-articles.xml.bz2

data/wikipedia.sled: data data/links.txt venv
	. venv/bin/activate && ./server.py
	cd inserter && cargo run --release
	. venv/bin/activate && ./server.py --stop

explorer: data/wikipedia.sled venv
	. venv/bin/activate && ./server.py
	. venv/bin/activate && python explorer.py
	. venv/bin/activate && ./server.py --stop

clean:
	rm -rf venv data
