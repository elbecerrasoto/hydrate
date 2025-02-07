#!/usr/bin/env python3

import subprocess as sp
import pandas as pd
import numpy as np

IN = "genomes.tsv"
TRIES = 12
BATCH_SIZE = 4

df = pd.read_table(IN)
genomes = np.array(df.genome)

batches = np.array_split(genomes, int(np.ceil(len(genomes) / BATCH_SIZE)))

for batch in batches:
    print(batch)

# parallel -l 256 'mkdir -p genomes/{#} && datasets download genome accession {} --dehydrated --filename genomes/{#}/{#}.zip --include protein,gff3 && unzip genomes/{#}/{#}.zip -d genomes/{#} && datasets rehydrate --directory genomes/{#}' --api-key  80e90a387605463df09ac9121d0caa0b7108 :::: redownload.txt\n

# while TRIES:
#     main()
#     TRIES -= i

# def sort_filter_genomes(inpath: Path, outpath: Path, only_refseq: bool) -> list[str]:
#     """
#     Given a input genome list (genome assembly accessions).
#     Generate a python list with valid ids.
#     The list is the input to the Snakemake hoox pipeline.

#      A tsv with the used ids is generated on a given location.
#     """

#     GENOME_REGEX = r"^GC[AF]_\d+\.\d+$"
#     REFSEQ_REGEX = r"^GCF_"
#     ID_REGEX = r"^GC[AF]_(\d+)\.\d+$"
#     VERSION_REGEX = r"^GC[AF]_\d+\.(\d+)$"

#     def remove_comments(x: str) -> str:
#         return re.sub(r"#.*$", "", x).strip()

#     df = pd.read_table(inpath, names=("genome",), sep="\t")
#     df.genome = df.genome.apply(remove_comments)

#     genome_matches = [bool(re.match(GENOME_REGEX, g)) for g in df.genome]

#     df = df.loc[genome_matches, :]

#     df["refseq"] = df.genome.apply(lambda x: bool(re.search(REFSEQ_REGEX, x)))
#     df["id"] = df.genome.apply(lambda x: int(re.search(ID_REGEX, x).group(1)))
#     df["version"] = df.genome.apply(lambda x: int(re.search(VERSION_REGEX, x).group(1)))

#     df = df.drop_duplicates()
#     df = df.sort_values(["id", "version", "refseq"], ascending=False)

#     if only_refseq:
#         df = df[df.refseq]

#     df = df.groupby("id").first()
#     df.to_csv(outpath, sep="\t")

#     return list(df.genome)


# def for_all_genomes(mark: str, results_genomes: Path, genomes: [str]) -> list[str]:
#     return [str(results_genomes / genome / f"{genome}{mark}") for genome in genomes]


# def bind_files(sm_input, sm_output, header):

#     sm_input = str(sm_input)
#     sm_output = str(sm_output)
#     header = str(header)

#     with open(sm_output, "w") as wfile:
#         wfile.write(header + "\n")
#         for path in sm_input.split(" "):
#             with open(path, "r") as rfile:
#                 for line in rfile:
#                     wfile.write(line)


# def is_internet_on():
#     # https://stackoverflow.com/questions/20913411/test-if-an-internet-connection-is-present-in-python
#     import socket

#     try:
#         socket.create_connection(("1.1.1.1", 53))
#         return True
#     except OSError:
#         return False


# if not is_internet_on():
#     print("No network connection.\nShutting down execution.")
#     sys.exit(1)


# DESCRIPTION = """Wrapper for ncbi-datasets-cli

#     under the hood:
#         datasets download genome accession --help

#     install:
#         mamba install -y -c conda-forge ncbi-datasets-cli

#     formats:
#          genome:     genomic sequence
#          rna:        transcript
#          protein:    amnio acid sequences
#          cds:        nucleotide coding sequences
#          gff3:       general feature file
#          gtf:        gene transfer format
#          gbff:       GenBank flat file
#          seq-report: sequence report file
#          none:       do not retrieve any sequence files
#          default [genome]
# """

# # An Example:

# # ├── temp_GCF_024145975.1
# # │   ├── GCF_024145975.1.zip
# # │   ├── ncbi_dataset
# # │   │   └── data
# # │   │       ├── assembly_data_report.jsonl
# # │   │       ├── dataset_catalog.json
# # │   │       └── GCF_024145975.1
# # │   │           ├── cds_from_genomic.fna
# # │   │           ├── GCF_024145975.1_ASM2414597v1_genomic.fna
# # │   │           ├── genomic.gbff
# # │   │           ├── genomic.gff
# # │   │           ├── genomic.gtf
# # │   │           ├── protein.faa
# # │   │           └── sequence_report.jsonl
# # │   └── README.md


# INCLUDE_DEF = ["genome", "protein", "gff3"]
# CWD = Path(os.getcwd())

# parser = argparse.ArgumentParser(
#     description=DESCRIPTION, formatter_class=argparse.RawDescriptionHelpFormatter
# )
# parser.add_argument("genome", help="NCBI accession")
# parser.add_argument("-o", "--out-dir", help="Default: new dir with accession number")
# parser.add_argument("-n", "--dry-run", action="store_true")
# parser.add_argument("-d", "--debug", action="store_true")
# parser.add_argument(
#     "-i",
#     "--include",
#     nargs="*",
#     choices=[
#         "genome",
#         "rna",
#         "protein",
#         "cds",
#         "gff3",
#         "gtf",
#         "gbff",
#         "seq-report",
#         "none",
#     ],
#     help=f"Which formats to download, Default: {INCLUDE_DEF}",
# )
# parser.add_argument(
#     "-p", "--particle", help="Rename particle, Default: genome NCBI accession"
# )
# parser.add_argument(
#     "-r",
#     "--no-rename",
#     action="store_false",
#     help="Don't rename output files, keep NCBI names",
# )
# parser.add_argument("-k", "--keep", action="store_false", help="Keep temporal files")
# args = parser.parse_args()


# GENOME = args.genome
# PARTICLE = GENOME if args.particle is None else args.particle

# RENAMES = {
#     "genome": (
#         r1 := re.compile(GENOME + r"_.*_genomic\.fna$"),
#         lambda x: re.sub(r1, f"{PARTICLE}.fna", x),
#     ),
#     "cds": (
#         r2 := re.compile(r"cds_from_genomic\.fna$"),
#         lambda x: re.sub(r2, f"{PARTICLE}_cds.fna", x),
#     ),
#     "gbff": (
#         r3 := re.compile(r"genomic\.gbff$"),
#         lambda x: re.sub(r3, f"{PARTICLE}.gbff", x),
#     ),
#     "gff3": (
#         r4 := re.compile(r"genomic\.gff$"),
#         lambda x: re.sub(r4, f"{PARTICLE}.gff", x),
#     ),
#     "gtf": (
#         r5 := re.compile(r"genomic\.gtf$"),
#         lambda x: re.sub(r5, f"{PARTICLE}.gtf", x),
#     ),
#     "protein": (
#         r6 := re.compile(r"protein\.faa$"),
#         lambda x: re.sub(r6, f"{PARTICLE}.faa", x),
#     ),
#     "sequence-report": (
#         r7 := re.compile(r"sequence_report\.jsonl$"),
#         lambda x: re.sub(r7, f"{PARTICLE}.jsonl", x),
#     ),
# }


# OUT_DIR = CWD / GENOME if args.out_dir is None else Path(args.out_dir)
# INCLUDE = ",".join(INCLUDE_DEF) if args.include is None else ",".join(args.include)

# DRY = args.dry_run
# DEBUG = args.debug
# NO_RENAME = args.no_rename
# KEEP = args.keep

# if DEBUG:
#     ic(args)

# if __name__ == "__main__":
#     TMP_DIR = CWD / f"temp_{GENOME}"
#     ZIP = TMP_DIR / (GENOME + ".zip")

#     DATASETS = split(
#         f"datasets download genome accession {GENOME} --no-progressbar --filename {ZIP} --include {INCLUDE}"
#     )
#     UNZIP = split(f"unzip -nq {ZIP} -d {TMP_DIR}")

#     if DEBUG:
#         ic(GENOME, OUT_DIR, INCLUDE, CWD, TMP_DIR, ZIP, DATASETS, UNZIP)

#     if not DRY:
#         TMP_DIR.mkdir(parents=True, exist_ok=True)
#         OUT_DIR.mkdir(parents=True, exist_ok=True)

#         sp.run(DATASETS, check=True)

#         sp.run(UNZIP, check=True)

#         NESTED = TMP_DIR / "ncbi_dataset" / "data" / GENOME
#         # rename downloaded data
#         if NO_RENAME:
#             for genome_data in NESTED.iterdir():
#                 for rename in RENAMES:
#                     genome_data = str(genome_data)

#                     test = RENAMES[rename][0]
#                     sub = RENAMES[rename][1]

#                     if re.search(test, genome_data):
#                         shutil.move(genome_data, sub(genome_data))
#                         break

#         # move downloaded data
#         for genome_data in NESTED.iterdir():
#             shutil.move(genome_data, OUT_DIR)

#         if KEEP:
#             shutil.rmtree(TMP_DIR)

#     else:
#         print("DRY RUN\nActions that would've run:\n")
#         print(f"mkdir -p {OUT_DIR}")
#         print(join(DATASETS))
#         print(join(UNZIP))
#         print(f"rm -r {TMP_DIR}")
