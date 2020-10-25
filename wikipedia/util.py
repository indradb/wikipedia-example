import sys
import uuid
import hashlib
import itertools

# Maximum number of items to return per chunk in `grouper`
MAX_CHUNK_SIZE = 50000

PROGRESS_BAR_LENGTH = 55
ERASE_LINE = '\x1b[2K'

def grouper(iterable, max_chunk_size=MAX_CHUNK_SIZE):
    """Via http://stackoverflow.com/a/8991553"""
    it = iter(iterable)
    while True:
       chunk = list(itertools.islice(it, MAX_CHUNK_SIZE))
       if not chunk:
           return
       yield chunk

def article_uuid(name):
    h = hashlib.blake2b(name.encode("utf8"), digest_size=16)
    return uuid.UUID(bytes=h.digest())

def progress(count, total):
    filled_len = int(round(PROGRESS_BAR_LENGTH * count / float(total)))
    percent = round(100.0 * count / float(total), 1)
    bar = "#" * filled_len + " " * (PROGRESS_BAR_LENGTH - filled_len)
    sys.stdout.write(ERASE_LINE)
    sys.stdout.write("[{}] {}% | {:.0f}/{:.0f}\r".format(bar, percent, count, total))
    sys.stdout.flush()
    return percent
