"""
This application will:

1) Decompress/parse a bzipped archive of wikipedia article data-on-the-fly
2) Find all the links in the article content to other wiki articles
3) Write the results to a TSV file
"""

import bz2
from xml.etree import ElementTree
import os
import re
import sys

import wikipedia

# Pattern for finding internal links in wikitext
WIKI_LINK_PATTERN = re.compile(r"\[\[([^\[\]|]+)(|[\]]+)?\]\]")

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
                            yield (title, match.group(1).replace("\n", "").replace("\t", ""))

                elem.clear()
    except EOFError:
        pass

def main(archive_path):
    archive_size_mb = os.stat(archive_path).st_size / 1024 / 1024

    streamer = ByteStreamer(archive_path)

    with open("data/links.tsv", "w") as f:
        for (src, dst) in iterate_page_links(streamer):
            mb_processed = streamer.read_bytes / 1024 / 1024
            f.write("{}\t{}\n".format(src, dst))
            wikipedia.progress(mb_processed, archive_size_mb)

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise Exception("No archive path specified")
    else:
        main(sys.argv[1])
