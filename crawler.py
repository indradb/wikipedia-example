"""
This application will:

1) Decompress/parse a bzipped archive of wikipedia article data-on-the-fly
2) Find all the links in the article content to other wiki articles
3) Create vertices/edges in IndraDB

Once completed, the wikipedia dataset will be explorable from briad.
"""

import bz2
from xml.etree import ElementTree
import re
import sys
import pickle
import wikipedia
import indradb

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

class ByteStreamer(object):
    """Streams decompressed bytes"""

    def __init__(self, path):
        self.path = path
        self.f = open(path, "rb")
        self.decompressor = bz2.BZ2Decompressor()

    def read(self, size):
        compressed_bytes = self.f.read(BYTE_STREAMER_BUFFER_SIZE)
        return self.decompressor.decompress(compressed_bytes)

def iterate_page_links(path):
    """Parses a stream of XML, and yields the article links"""

    streamer = ByteStreamer(path)
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

def insert_articles(client, article_names_to_ids, links_chunk):
    """
    From a batch of links, this finds all the unique articles, inserts them
    into IndraDB.
    """
    new_article_names = set([])

    # Find all of the unique article names that haven't been inserted before
    for (from_article_name, to_article_name) in links_chunk:
        if from_article_name not in article_names_to_ids:
            new_article_names.add(from_article_name)
        if to_article_name not in article_names_to_ids:
            new_article_names.add(to_article_name)

    # Create the articles in IndraDB, and get a mapping of article names to
    # their vertex IDs
    trans = indradb.Transaction()

    for _ in new_article_names:
        trans.create_vertex_from_type(type="article")

    new_article_names_mapping = list(zip(new_article_names, client.transaction(trans)))

    # Set the metadata on the vertices
    trans = indradb.Transaction()

    for (article_name, article_id) in new_article_names_mapping:
        trans.set_vertex_metadata(indradb.VertexQuery.vertices([article_id]), "name", article_name)

    client.transaction(trans)
    
    # Update the in-memory mapping
    for (article_name, article_id) in new_article_names_mapping:
        article_names_to_ids[article_name] = article_id

    return len(new_article_names)

def insert_links(client, article_names_to_ids, links_chunk):
    """
    From a batch of links, this inserts all of the links into briad
    """

    # Create the links in IndraDB in batches
    trans = indradb.Transaction()

    for (from_article_name, to_article_name) in links_chunk:
        trans.create_edge(indradb.EdgeKey(
            article_names_to_ids[from_article_name],
            "link",
            article_names_to_ids[to_article_name],
        ))

    client.transaction(trans)
    return len(trans.payload)

def main(archive_path):
    """Parses article links and stores results in a `shelve` database"""

    article_names_to_ids = {}

    with wikipedia.server():
        client = wikipedia.get_client()

        for links_chunk in wikipedia.grouper(iterate_page_links(archive_path)):
            num_articles_inserted = insert_articles(client, article_names_to_ids, links_chunk)
            print("%s articles inserted" % num_articles_inserted)
            num_links_inserted = insert_links(client, article_names_to_ids, links_chunk)
            print("%s links inserted" % num_links_inserted)

    with open("data/article_names_to_ids.pickle", "wb") as f:
        pickle.dump(article_names_to_ids, f, pickle.HIGHEST_PROTOCOL)

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise Exception("No archive path specified")
    else:
        main(sys.argv[1])
