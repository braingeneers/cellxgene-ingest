"""
Ingest cellxgene data and upload to s3 in parallel using ray disributed on k8s
"""

import sys
import os
import argparse
import logging
import pandas as pd
import ray

import boto3
import cellxgene_census

parser = argparse.ArgumentParser(
    description=__doc__,
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument("--census-version", default="2023-12-15")
parser.add_argument("--index", default="index.feather")
parser.add_argument("-n", "--max-num-observations", type=int, default=None)
parser.add_argument("-c", "--observations-per-file", type=int, default=100)
parser.add_argument("-d", "--max-parallel-downloads", type=int, default=None)
parser.add_argument("--bucket", type=str, default="braingeneers")
parser.add_argument(
    "--gene-filter", default=None, help="ex. ENSG00000161798,ENSG00000139618"
)
parser.add_argument("dest", type=str, nargs="?", default="cellxgene")
args = parser.parse_args()

if ray.is_initialized():
    ray.shutdown()

ray.init(num_cpus=args.max_parallel_downloads, ignore_reinit_error=True)

df = pd.read_feather(args.index, columns=["soma_joinid"])
if args.max_num_observations:
    df = df.sample(n=args.max_num_observations, random_state=42)
ds = ray.data.from_pandas(df)
ds = ds.materialize().repartition(4 * args.max_parallel_downloads)
# ds = ds.materialize()


# @ray.remote(scheduling_strategy="SPREAD")
class BatchIngestor:
    """Download a batch of observations and upload as a single h5ad file to s3"""

    def __init__(self, args):
        # print("Creating actor")
        self.args = args
        self.census = cellxgene_census.open_soma(census_version=args.census_version)
        self.s3 = boto3.client("s3")

        logger = logging.getLogger("ray_logger")
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(process)s %(asctime)s | %(levelname)s | %(message)s"
        )

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setLevel(logging.DEBUG)
        stdout_handler.setFormatter(formatter)

        # file_handler = logging.FileHandler('ray_train_logs.log')
        # file_handler.setLevel(logging.DEBUG)
        # file_handler.setFormatter(formatter)

        logger.handlers = []
        # logger.addHandler(file_handler)
        logger.addHandler(stdout_handler)
        self.logger = logger

        # self.logger.info("Created BatchIngestor")

    def __call__(self, batch: dict) -> dict:
        # self.logger.info(f"Processing {len(batch['soma_joinid'])} observations")
        import time
        import random

        # Wait between 0 and 1 second
        time.sleep(random.randint(0, 10) / 10)
        return {
            "id": batch["soma_joinid"],
            "pid": [os.getpid() for _ in range(len(batch["soma_joinid"]))],
        }


results = ds.map_batches(
    BatchIngestor,
    zero_copy_batch=True,
    batch_size=args.observations_per_file,
    concurrency=(4, args.max_parallel_downloads),
    fn_constructor_args=(args,),
)

b = results.take_all()

from collections import Counter

print(Counter([r["pid"] for r in b]))
