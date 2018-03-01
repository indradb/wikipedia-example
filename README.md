# IndraDB wikipedia example

This example webapp uses [IndraDB](https://github.com/indradb/indradb) to explore the links in wikipedia articles.

## Getting started

* Make sure you have python 3 installed.
* Make sure you have IndraDB installed, and that the applications are available in your `PATH`.
* Clone the repo.
* Get a copy of the [latest wikipedia data](https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2) and download it into the root directory of the repo.
* Run `make`. The run will take a long time, as several GB worth of wikipedia data needs to be indexed in IndraDB. But subsequent invocations of `make` will be snappy.
