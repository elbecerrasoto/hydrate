#!/usr/bin/env python3

import subprocess as sp
from multiprocessing import Pool
import pandas as pd
import numpy as np
import shlex

IN = "genomes.tsv"
TRIES = 12
BATCH_SIZE = 4
VERBOSE = True
KEY = "80e90a387605463df09ac9121d0caa0b7108"

DEHYDRATE_LEAD = ["datasets", "download", "genome", "accession"]
DEHYDRATE_LAG = ["--dehydrated", "--include", "protein,gff3", "--api-key", f"{KEY}"]
REHYDRATE_LEAD = ["datasets", "rehydrate", "--api-key", f"{KEY}"]


def worker(batch):
    idx, genomes = batch

    dehydrated_zip = f"genomes/{idx}/{idx}.zip"
    rehydrated_dir = f"genomes/{idx}"

    dehydrate_cmd = (
        DEHYDRATE_LEAD + list(genomes) + ["--filename", dehydrated_zip] + DEHYDRATE_LAG
    )
    unzip_cmd = ["unzip", dehydrated_zip, "-d", rehydrated_dir]
    rehydrate_cmd = REHYDRATE_LEAD + ["--directory", rehydrated_dir]

    return dehydrate_cmd, dehydrate_cmd, unzip_cmd


if __name__ == "__main__":

    df = pd.read_table(IN)
    genomes = np.array(df.genome)

    batches = np.array_split(genomes, int(np.ceil(len(genomes) / BATCH_SIZE)))
    batches = tuple(enumerate(batches))

    with Pool() as p:
        results = p.map(worker, batches)

    print(results)
