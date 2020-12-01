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

    src = None
    content = None
    blacklisted = False
    linked = set()
    is_tag = lambda elem, name: elem.tag == "{%s}%s" % (EXPORT_NAMESPACE, name)

    try:
        for event, elem in ElementTree.iterparse(streamer, events=("start", "end")):
            if event == "start":
                if is_tag(elem, "page"):
                    src = None
                    content = None
                    blacklisted = False
                    linked = set()
            elif event == "end":
                if not blacklisted:
                    if is_tag(elem, "title"):
                        assert src is None
                        src = elem.text

                        if any(src.startswith(p) for p in ARTICLE_NAME_PREFIX_BLACKLIST):
                            blacklisted = True
                    elif is_tag(elem, "text"):
                        assert content is None
                        content = elem.text.strip() if elem.text else ""

                        if content.startswith("#REDIRECT [["):
                            blacklisted = True
                    elif is_tag(elem, "page"):
                        assert src is not None
                        assert content is not None

                        for match in re.finditer(WIKI_LINK_PATTERN, content):
                            dst = match.group(1).replace("\n", "").replace("\t", "")

                            if dst not in linked:
                                yield (src, dst)
                                linked.add(dst)

                elem.clear()
    except EOFError:
        pass

def progress(count, total):
    # convert to mb
    count /= (1024 * 1024)
    total /= (1024 * 1024)

    percent = round(100.0 * count / float(total), 1)

    sys.stdout.write(ERASE_LINE)
    sys.stdout.write("[{}] {}% | {:.0f}/{:.0f}\r".format(percent, count, total))
    sys.stdout.flush()

def main(archive_path):
    archive_size = os.stat(archive_path).st_size
    streamer = ByteStreamer(archive_path)
    cur_src = None

    with open("data/links.txt", "w") as f:
        for (src, dst) in iterate_page_links(streamer):
            progress(streamer.read_bytes, archive_size)

            if src != cur_src:
                f.write(src + "\n")
                cur_src = src
            f.write("\t" + dst + "\n")

if __name__ == "__main__":
    if len(sys.argv) <= 1:
        raise Exception("No archive path specified")
    else:
        main(sys.argv[1])
