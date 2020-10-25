export SECRET=OME88YorohonzPNWEFsi0dIsouXWqeO$
export DATABASE_URL=rocksdb://data/wikipedia.rdb
export RUST_BACKTRACE=1

.PHONY: explorer

explorer: venv data
	. venv/bin/activate && python explorer.py

venv:
	virtualenv -p python3 venv
	. venv/bin/activate && pip install -r requirements.txt

data: venv
	mkdir -p data
	. venv/bin/activate && python crawler.py enwiki-latest-pages-articles.xml.bz2
	. venv/bin/activate && python inserter.py

clean:
	rm -rf venv data
