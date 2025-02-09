#!/usr/bin/env python

import os
import subprocess as sp
from pathlib import Path
from multiprocessing import Process


TARGETS = sys.argv[1:]
TARGETS = [Path(t) for t in TARGETS]
ENCODING = "utf-8"


def create_fifo(path):
    path = Path(path)
    if not path.exists():
        os.mkfifo(path)
    if not path.is_fifo():
        raise FileExistsError(f"{path} already exists and it is not a FIFO.")
    return path


def worker(fifo):
    with open(fifo, "w", endcoding=ENCODING) as hfifo:
        completed = sp.run(
            ["pigz", "-dc", str(out)],
            capture_output=True,
            check=True,
            encoding=ENCODING,
        )
        hfifo.write(completed)


if __name__ == "__main__":

    outs = [t.parent / t.stem for t in TARGETS]
    procs = []

    for out in outs:

        fifo = create_fifo(out)
        p = Process(target=worker, args=(fifo))
        procs.append(p)

        p.start()
