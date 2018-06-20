import indradb

# Location of the IndraDB server
HOST_CONFIG = "localhost:27615"

def get_client():
    return indradb.Client(HOST_CONFIG)
