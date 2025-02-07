#!/usr/bin/env python3

import subprocess as sp
import pandas as pd
import numpy as np
import shlex

IN = "genomes.tsv"
TRIES = 12
BATCH_SIZE = 4
VERBOSE = True
KEY = "80e90a387605463df09ac9121d0caa0b7108"

df = pd.read_table(IN)
genomes = np.array(df.genome)

batches = np.array_split(genomes, int(np.ceil(len(genomes) / BATCH_SIZE)))


DEHYDRATE_LEAD = ["datasets", "download", "genome", "accession"]
DEHYDRATE_LAG = ["--dehydrated", "--include", "protein,gff3", "--api-key", f"{KEY}"]

REHYDRATE_LEAD = ["datasets", "rehydrate", "--api-key", f"{KEY}"]

CMDS_DEHYDRATE = []
CMDS_UNZIP = []
CMDS_REHYDRATE = []

for idx, batch in enumerate(batches):
    dehydrated_zip = f"genomes/{idx}/{idx}.zip"
    rehydrated_dir = f"genomes/{idx}"

    idehydrate = (
        DEHYDRATE_LEAD + list(batch) + ["--filename", dehydrated_zip] + DEHYDRATE_LAG
    )
    iunzip = ["unzip", dehydrated_zip, "-d", rehydrated_dir]
    irehydrate = REHYDRATE_LEAD + ["--directory", rehydrated_dir]

    if VERBOSE:
        print(shlex.join(idehydrate))
        print(shlex.join(iunzip))
        print(shlex.join(irehydrate))

    CMDS_DEHYDRATE.append(idehydrate)
    CMDS_UNZIP.append(idehydrate)
    CMDS_REHYDRATE.append(irehydrate)
