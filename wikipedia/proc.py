#!/usr/bin/env python

import requests
import re
import sys
import time
import subprocess
from contextlib import contextmanager

@contextmanager
def _http_server(process, url):
    """
    Context manager for running an HTTP server. This starts the process, waits
    until its responsive, then yields. When the context manager's execution is
    resumed, it kills the process.
    """

    # Start the process
    proc = subprocess.Popen(process, stdout=sys.stdout, stderr=sys.stderr)
    
    while True:
        # Check if the server is now responding to HTTP requests
        try:
            res = requests.get(url, timeout=1)

            if res.status_code == 200 or res.status_code == 404:
                break
        except requests.exceptions.RequestException:
            pass

        # Server is not yet responding to HTTP requests - let's make sure it's
        # running in the first place
        if proc.poll() != None:
            raise Exception("Server failed to start")

        time.sleep(1)

    try:
        yield
    finally:
        proc.terminate()


def server():
    """Starts the IndraDB server"""
    return _http_server("indradb-server", "http://localhost:8000")

def dashboard():
    """Starts the IndraDB dashboard"""
    return _http_server("indradb-dashboard", "http://localhost:27615")
