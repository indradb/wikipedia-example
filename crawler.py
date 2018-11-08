"""
This application will:

1) Decompress/parse a bzipped archive of wikipedia article data-on-the-fly
2) Find all the links in the article content to other wiki articles
3) Create vertices/edges in IndraDB

Once completed, the wikipedia dataset will be explorable from briad.
"""

import bz2
from xml.etree import ElementTree
import os
import re
import sys
import time
import uuid
import pickle
import shelve

import capnp
import wikipedia
import indradb

PROGRESS_BAR_LENGTH = 55

# Pattern for finding internal links in wikitext
WIKI_LINK_PATTERN = re.compile(r"\[\[([^\[\]|]+)(|[\]]+)?\]\]")

# Valid URL patterns
URL_PATTERN = re.compile(
    r'^(?:http)s?://' # http:// or https://
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
    r'localhost|' #localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
    r'(?::\d+)?' # optional port
    r'(?:/?|[/?]\S+)$', re.IGNORECASE
)

# Size of the `ByteStreamer` buffer
BYTE_STREAMER_BUFFER_SIZE = 1024 * 1024 * 10

# Mediawiki xml namespace
EXPORT_NAMESPACE = "http://www.mediawiki.org/xml/export-0.10/"

# Prefixes of articles to ignore
ARTICLE_NAME_PREFIX_BLACKLIST = [
    "Wikipedia:",
    "WP:",
    ":",
    "File:",
    "Image:",
    "Template:",
    "User:",
]

ERASE_LINE = '\x1b[2K'

class ByteStreamer(object):
    """Streams decompressed bytes"""

    def __init__(self, path):
        self.path = path
        self.f = open(path, "rb")
        self.decompressor = bz2.BZ2Decompressor()
        self.read_bytes = 0

    def read(self, size):
        compressed_bytes = self.f.read(BYTE_STREAMER_BUFFER_SIZE)
        self.read_bytes += BYTE_STREAMER_BUFFER_SIZE
        return self.decompressor.decompress(compressed_bytes)

def iterate_page_links(streamer):
    """Parses a stream of XML, and yields the article links"""

    title = None
    content = None
    blacklisted = False
    is_tag = lambda elem, name: elem.tag == "{%s}%s" % (EXPORT_NAMESPACE, name)

    try:
        for event, elem in ElementTree.iterparse(streamer, events=("start", "end")):
            if event == "start":
                if is_tag(elem, "page"):
                    title = None
                    content = None
                    blacklisted = False
            elif event == "end":
                if not blacklisted:
                    if is_tag(elem, "title"):
                        assert title is None
                        title = elem.text

                        if any(title.startswith(p) for p in ARTICLE_NAME_PREFIX_BLACKLIST):
                            blacklisted = True
                    elif is_tag(elem, "text"):
                        assert content is None
                        content = elem.text.strip() if elem.text else ""

                        if content.startswith("#REDIRECT [["):
                            blacklisted = True
                    elif is_tag(elem, "page"):
                        assert title is not None
                        assert content is not None

                        for match in re.finditer(WIKI_LINK_PATTERN, content):
                            yield (title, match.group(1).replace("\n", ""))

                elem.clear()
    except EOFError:
        pass

class Inserter:
    def __init__(self, client):
        self.client = client
        self.article_names_to_ids = {}

    def articles(self, links_chunk):
        """
        From a batch of links, this finds all the unique articles, inserts
        them into IndraDB.
        """
        new_article_names = set([])

        # Find all of the unique article names that haven't been inserted before
        for (from_article_name, to_article_name) in links_chunk:
            if from_article_name not in self.article_names_to_ids:
                new_article_names.add(from_article_name)
            if to_article_name not in self.article_names_to_ids:
                new_article_names.add(to_article_name)

        # Create the articles in IndraDB
        items = []

        for article_name in new_article_names:
            vertex_id = uuid.uuid1()
            items.append(indradb.BulkInsertVertex(indradb.Vertex(vertex_id, "article")))
            items.append(indradb.BulkInsertVertexProperty(vertex_id, "name", article_name))
            self.article_names_to_ids[article_name] = vertex_id

        return self.client.bulk_insert(items)

    def links(self, links_chunk):
        """
        From a batch of links, this inserts all of the links into IndraDB.
        """

        items = []

        for (from_article_name, to_article_name) in links_chunk:
            edge_key = indradb.EdgeKey(
                self.article_names_to_ids[from_article_name],
                "link",
                self.article_names_to_ids[to_article_name],
            )

            items.append(indradb.BulkInsertEdge(edge_key))

        return self.client.bulk_insert(items)

def progress(count, total):
    filled_len = int(round(PROGRESS_BAR_LENGTH * count / float(total)))
    percent = round(100.0 * count / float(total), 1)
    bar = "#" * filled_len + " " * (PROGRESS_BAR_LENGTH - filled_len)
    sys.stdout.write(ERASE_LINE)
    sys.stdout.write("[{}] {}% | {:.0f}/{:.0f}\r".format(bar, percent, count, total))
    sys.stdout.flush()

def main(archive_path):
    last_promise = capnp.join_promises([]) # Create an empty promise
    start_time = time.time()
    archive_size_mb = os.stat(archive_path).st_size / 1024 / 1024

    with wikipedia.server(bulk_load_optimized=True) as client:
        inserter = Inserter(client)
        streamer = ByteStreamer(archive_path)
        print("Decompressing and indexing content...")

        # Now insert the articles and links iteratively
        for links_chunk in wikipedia.grouper(iterate_page_links(streamer)):
            last_promise.wait()
            inserter.articles(links_chunk).wait()
            last_promise = inserter.links(links_chunk)
            cur_time = time.time()
            mb_processed = streamer.read_bytes / 1024 / 1024
            progress(mb_processed, archive_size_mb)

        last_promise.wait()

        print("\nDumping results...")

        with shelve.open("data/article_names_to_ids.shelve") as persisted_article_names_to_ids:
            persisted_article_names_to_ids.update(inserter.article_names_to_ids)

        print("Done!")

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise Exception("No archive path specified")
    else:
        main(sys.argv[1])
