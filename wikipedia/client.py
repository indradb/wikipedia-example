import indradb

# Location of the IndraDB server
HOST_CONFIG = ("localhost", 8000)

# How long in seconds before an IndraDB request times out
REQUEST_TIMEOUT = 600

def get_client():
    return indradb.Client(HOST_CONFIG, request_timeout=REQUEST_TIMEOUT, scheme="http")
