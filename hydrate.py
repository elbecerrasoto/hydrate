#!/usr/bin/env python3

import os
import random
import shutil
import subprocess as sp
import time
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import pandas as pd

IN = "r.tsv"
BATCH_SIZE = 256
TRIES = 12
KEY = "80e90a387605463df09ac9121d0caa0b7108"
WORKERS_DOWNLOAD = os.cpu_count() * 10

DEHYDRATE_LEAD = ["datasets", "download", "genome", "accession"]
DEHYDRATE_LAG = ["--dehydrated", "--include", "protein,gff3", "--api-key", f"{KEY}"]
REHYDRATE_LEAD = ["datasets", "rehydrate", "--api-key", f"{KEY}"]


def worker(idx, genomes):
    time.sleep(0.1 + random.randint(0, 3))
    unsuccessful_genomes = []

    batch_dir = Path(f"genomes/batches/{idx}")
    batch_zip = batch_dir / f"{idx}.zip"

    dehydrate_cmd = (
        DEHYDRATE_LEAD + list(genomes) + ["--filename", str(batch_zip)] + DEHYDRATE_LAG
    )
    unzip_cmd = ["unzip", str(batch_zip), "-d", str(batch_dir)]
    rehydrate_cmd = REHYDRATE_LEAD + ["--directory", str(batch_dir)]
    md5sum_cmd = ["md5sum", "-c", "md5sum.txt"]

    try:

        # Batch Processing
        batch_dir.mkdir(parents=True)

        sp.run(dehydrate_cmd, check=True)
        sp.run(unzip_cmd, check=True)
        sp.run(rehydrate_cmd, check=True)
        sp.run(md5sum_cmd, check=True, cwd=batch_dir)

    except (sp.CalledProcessError, FileExistsError) as err:
        print(err)

    for genome in genomes:

        genome_dir = Path(f"genomes/{genome}")
        gff = batch_dir / "ncbi_dataset" / "data" / genome / "genomic.gff"
        faa = batch_dir / "ncbi_dataset" / "data" / genome / "protein.faa"

        print(gff)

        if gff.is_file() and faa.is_file():
            genome_dir.mkdir()
            gff = gff.rename(genome_dir / f"{genome}.gff")
            faa = faa.rename(genome_dir / f"{genome}.faa")

            sp.run(["pigz", str(gff), str(faa)], check=True)
        else:
            unsuccessful_genomes.append(genome)

    return unsuccessful_genomes


def download(genomes: list[str]):

    batches = np.array_split(genomes, int(np.ceil(len(genomes) / BATCH_SIZE)))
    batches = tuple(enumerate(batches))

    batches_dir = Path(f"genomes/batches")
    batches_dir.mkdir(parents=True)

    with Pool(WORKERS_DOWNLOAD) as p:
        results = p.starmap(worker, batches)

    shutil.rmtree(batches_dir)

    unsuccessful_genomes = []
    for result in results:
        unsuccessful_genomes.extend(result)

    return unsuccessful_genomes


if __name__ == "__main__":

    df = pd.read_table(IN)
    genomes = list(df.genome)

    remaining_genomes = genomes
    for i in range(TRIES):
        remaining_genomes = download(remaining_genomes)
        if not remaining_genomes:
            break
