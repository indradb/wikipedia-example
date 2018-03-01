import itertools

# Maximum number of items to return per chunk in `grouper`
MAX_CHUNK_SIZE = 50000

def grouper(iterable):
    """Via http://stackoverflow.com/a/8991553"""
    it = iter(iterable)
    while True:
       chunk = list(itertools.islice(it, MAX_CHUNK_SIZE))
       if not chunk:
           return
       yield chunk
