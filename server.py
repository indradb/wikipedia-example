#!/usr/bin/env python3

import os
import re
import sys
import time
import signal
import socket
import argparse
import subprocess
from contextlib import contextmanager

import indradb

HOST_CONFIG = "localhost:27615"
SECRET = "OME88YorohonzPNWEFsi0dIsouXWqeO$"
DATABASE_URL = "rocksdb://data/wikipedia.rdb"
# avoiding /var/run because it requires root
PID_FILE = "/tmp/indradb-wikipedia-example.pid"

def start(bulk_load_optimized):
    if os.path.isfile(PID_FILE):
        raise Exception("server appears to be running, as '{}' already exists".format(PID_FILE))

    env = dict(os.environ)
    env["SECRET"] = SECRET
    env["DATABASE_URL"] = DATABASE_URL
    env["RUST_BACKTRACE"] = "1"

    if bulk_load_optimized:
        env["ROCKSDB_BULK_LOAD_OPTIMIZED"] = "true"

    server_proc = subprocess.Popen(["indradb"], stdout=sys.stdout, stderr=sys.stderr, env=env)
    
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
            raise Exception("server failed to start")

        time.sleep(1)

    with open(PID_FILE, "w") as f:
        f.write(str(server_proc.pid))

def stop():
    if not os.path.isfile(PID_FILE):
        return False

    with open(PID_FILE, "r") as f:
        pid = int(f.read().strip())
        
        try:
            os.kill(pid, signal.SIGTERM)
        finally:
            os.remove(PID_FILE)

    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bulk-load-optimized", action="store_true", help="Optimize for bulk loading")
    parser.add_argument("--stop", action="store_true", help="Stop a running server")
    args = parser.parse_args()

    if args.stop:
        if not stop():
            raise Exception("could not find server to stop")
    else:
        start(args.bulk_load_optimized)

if __name__ == "__main__":
    main()
