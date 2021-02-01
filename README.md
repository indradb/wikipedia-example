# IndraDB wikipedia example

This example webapp uses [IndraDB](https://github.com/indradb/indradb) to explore the links in wikipedia articles.

## Getting started

* Make sure you have rust installed.
* Clone the repo.
* Run `make init explore`. This will take a long time, as it'll (1) download the wikipedia dataset if you don't have it already, (2) decompress and parse the archive, and (3) index the content into IndraDB. But subsequent invocations of `make` will be snappy.
