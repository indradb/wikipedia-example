export SECRET=OME88YorohonzPNWEFsi0dIsouXWqeO$
export DATABASE_URL=sled://data/wikipedia.sled
export RUST_BACKTRACE=1
export SLEDDB_COMPRESSION=true

.PHONY: explorer

explorer: venv data
	. venv/bin/activate && python explorer.py

venv:
	virtualenv -p python3 venv
	. venv/bin/activate && pip install -r requirements.txt

data: venv
	mkdir -p data
	. venv/bin/activate && time python crawler.py enwiki-latest-pages-articles.xml.bz2

clean:
	rm -rf venv data
