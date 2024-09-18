"""
Ingest cellxgene data and upload to s3 in parallel using Ray with manual Actor management
"""

import os
import time
import argparse
import pandas as pd
import ray
import tqdm

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


df = pd.read_feather(args.index, columns=["soma_joinid"])
soma_ids = df.soma_joinid[0 : args.max_num_observations].values
chunks = [
    soma_ids[start_soma_id : start_soma_id + args.observations_per_file]
    for start_soma_id in range(0, len(soma_ids), args.observations_per_file)
]
print(
    f"Downloading {len(soma_ids):,} observations in {len(chunks):,} files to s3://{args.bucket}/{args.dest}/"
)
if args.gene_filter:
    print(f"Filtering for genes: {args.gene_filter}")

if ray.is_initialized():
    ray.shutdown()

ray.init(num_cpus=args.max_parallel_downloads, ignore_reinit_error=True)


@ray.remote
class IngestBatch(object):
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

    def ingest(self, chunk):
        key = f"{self.args.dest}/{str(chunk[0])}-{str(chunk[-1])}.h5ad"
        bytes_received = 0
        if self.exists(key):
            print(f"{key} exists, skipping.")
        else:
            # print(f"{os.getpid()} Downloading {chunk}...")
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
                    obs_coords=chunk,
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
                    bytes_received = os.path.getsize(f.name)
                    s3 = boto3.client("s3")
                    s3.upload_file(f.name, "braingeneers", key)
        return (os.getpid(), bytes_received, chunk)


print(f"Creating pool of {args.max_parallel_downloads} ray actors...")
pool = ray.util.ActorPool(
    [IngestBatch.remote(args) for _ in range(args.max_parallel_downloads)]
)

print("Ingesting...")
start_time = time.time()
results = list(
    tqdm.tqdm(pool.map(lambda a, v: a.ingest.remote(v), chunks), total=len(chunks))
)
end_time = time.time()
print("Done.")

elapsed_seconds = end_time - start_time
print(f"{len(results):,} files ingested in {elapsed_seconds/60:.2f} minutes.")
print(f"{elapsed_seconds/3600/len(soma_ids)*1e6:.2f} hours per 1M observations.")
average_file_size = sum([r[1] for r in results]) / len(results)
print(f"{average_file_size/1e6:.2f} MB average file size.")
