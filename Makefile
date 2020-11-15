.PHONY: explorer

explorer: venv data
	. venv/bin/activate && ./server.py
	. venv/bin/activate && python explorer.py
	. venv/bin/activate && ./server.py --stop

venv:
	virtualenv -p python3 venv
	. venv/bin/activate && pip install -r requirements.txt

data: venv
	mkdir -p data
	. venv/bin/activate && python crawler.py enwiki-latest-pages-articles.xml.bz2
	. venv/bin/activate && ./server.py --bulk-load-optimized
	cd inserter && cargo run --release
	. venv/bin/activate && ./server.py --stop

clean:
	rm -rf venv data
