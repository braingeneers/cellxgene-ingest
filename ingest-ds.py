"""
Ingest cellxgene data and upload to s3 in parallel using ray disributed on k8s
"""

import os
import argparse
from collections import Counter
import pandas as pd
import ray

import botocore
import boto3
import tempfile
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
ds = ray.data.from_pandas(df)
if args.max_num_observations:
    ds = ds.limit(args.max_num_observations)
ds = ds.repartition(4 * args.max_parallel_downloads)


class BatchIngestor:
    """Download a batch of observations and upload as a single h5ad file to s3"""

    def __init__(self, args):
        self.args = args
        self.census = cellxgene_census.open_soma(census_version=args.census_version)
        self.s3 = boto3.client("s3")

    def exists(self, key):
        try:
            self.s3.head_object(Bucket=self.args.bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            return int(e.response["Error"]["Code"]) != 404
        return True

    def __call__(self, batch: dict) -> dict:
        key = f"{self.args.dest}/{str(batch['soma_joinid'][0])}-{str(batch['soma_joinid'][-1])}.h5ad"
        if self.exists(key):
            print(f"{key} exists, skipping.")
        else:
            if self.args.gene_filter:
                genes = ",".join([f"'{g}'" for g in self.args.gene_filter.split(",")])
                var_value_filter = f"feature_id in [{genes}]"
            else:
                var_value_filter = None

            with cellxgene_census.open_soma(
                census_version=self.args.census_version
            ) as census:
                anndata = cellxgene_census.get_anndata(
                    census=census,
                    organism="Homo sapiens",
                    var_value_filter=var_value_filter,
                    obs_coords=batch["soma_joinid"],
                    column_names={
                        "obs": [
                            "soma_joinid",
                        ],
                        "var": [
                            "soma_joinid",
                            "feature_id",
                            "feature_name",
                            "feature_length",
                        ],
                    },
                )
                with tempfile.NamedTemporaryFile() as f:
                    anndata.write_h5ad(f.name)
                    s3 = boto3.client("s3")
                    s3.upload_file(f.name, "braingeneers", key)
        return {
            "id": batch["soma_joinid"],
            "pid": [os.getpid() for _ in range(len(batch["soma_joinid"]))],
        }


results = ds.map_batches(
    BatchIngestor,
    zero_copy_batch=True,
    batch_size=args.observations_per_file,
    concurrency=(1, args.max_parallel_downloads),
    fn_constructor_args=(args,),
).take_all()

print(Counter([r["pid"] for r in results]))
