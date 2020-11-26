#!/usr/bin/env python

import os
import re
import sys
import time
import socket
import subprocess
from contextlib import contextmanager

import indradb

HOST_CONFIG = "localhost:27615"

@contextmanager
def server():
    """
    Context manager for running the server. This starts the server up, waits
    until its responsive, then yields. When the context manager's execution is
    resumed, it kills the server.
    """

    # Start the process
    server_proc = subprocess.Popen(
        ["indradb"],
        stdout=sys.stdout, stderr=sys.stderr, env=os.environ
    )
    
    while True:
        try:
            client = indradb.Client(HOST_CONFIG)

            if client.ping().wait().ready:
                break
        except ConnectionRefusedError as e:
            print(e)
        except socket.error as e:
            print(e)

        # Server is not yet responding to requests - let's make sure it's
        # running in the first place
        if server_proc.poll() != None:
            raise Exception("Server failed to start")

        time.sleep(1)

    try:
        yield client
    finally:
        server_proc.terminate()
