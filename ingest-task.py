"""
Ingest cellxgene data and upload to s3 in parallel using ray
"""

import argparse
import tqdm
import pandas as pd
import ray

if __name__ == "__main__":
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

    print(f"Starting Ray cluster with {args.max_parallel_downloads} cpus")
    ray.init(num_cpus=args.max_parallel_downloads, ignore_reinit_error=True)

    df = pd.read_feather(args.index)
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

    @ray.remote
    def ingest_chunk(chunk, args):
        import cellxgene_census
        import boto3
        import tempfile

        if args.gene_filter:
            genes = ",".join([f"'{g}'" for g in args.gene_filter.split(",")])
            var_value_filter = f"feature_id in [{genes}]"
        else:
            var_value_filter = None

        with cellxgene_census.open_soma(census_version=args.census_version) as census:
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
            name = f"{str(chunk[0])}-{str(chunk[-1])}.h5ad"
            with tempfile.NamedTemporaryFile() as f:
                anndata.write_h5ad(f.name)
                s3 = boto3.client("s3")
                s3.upload_file(f.name, "braingeneers", f"{args.dest}/{name}")
            return anndata

    futures = [ingest_chunk.remote(chunk, args) for chunk in chunks]

    progress_bar = tqdm.trange(len(chunks))
    anndatas = []
    while futures:
        finished, futures = ray.wait(futures)
        anndatas.append(ray.get(finished[0]))
        progress_bar.update(len(finished))
    progress_bar.close()

    ray.shutdown()
    print("Done.")
