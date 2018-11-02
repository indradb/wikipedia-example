#!/usr/bin/env python

import os
import re
import sys
import time
import socket
import subprocess
from contextlib import contextmanager

from .client import get_client

@contextmanager
def server(bulk_load_optimized=False):
    """
    Context manager for running the server. This starts the server up, waits
    until its responsive, then yields. When the context manager's execution is
    resumed, it kills the server.
    """

    # Start the process
    env = dict(os.environ)

    if bulk_load_optimized:
        env["ROCKSDB_BULK_LOAD_OPTIMIZED"] = "true"

    server_proc = subprocess.Popen(["indradb"], stdout=sys.stdout, stderr=sys.stderr, env=env)
    
    while True:
        try:
            client = get_client()
            
            if client.ping().wait().ready:
                break
        except socket.error as e:
            print(e)

        # Server is not yet responding to requests - let's make sure it's
        # running in the first place
        if server_proc.poll() != None:
            raise Exception("Server failed to start")

        time.sleep(1)

    try:
        yield
    finally:
        server_proc.terminate()

