#!/usr/bin/env python

import os
import sys
import subprocess as sp
from pathlib import Path
from multiprocessing import Process
import time


TARGETS = sys.argv[1:]
TARGETS = [Path(t) for t in TARGETS]
ENCODING = "utf-8"


def wait_for_reader(fifo):
    while not fifo.exists():
        time.sleep(0.1)


def worker(target):
    fifo = target.parent / target.stem
    wait_for_reader(fifo)
    with open(fifo, "w", encoding=ENCODING) as hfifo:
        completed = sp.run(
            ["pigz", "-dc", str(target)],
            capture_output=True,
            check=True,
            encoding=ENCODING,
        )
        # I need a reader before writing.
        # If not a Broken pipe error would be raised
        hfifo.write(completed.stdout)
        hfifo.flush()

if __name__ == "__main__":

    procs = []
    for target in TARGETS:
        p = Process(target=worker, args=(target,))
        procs.append(p)
        p.start()

    for p in procs:
        p.join() # blocks until it gets read
