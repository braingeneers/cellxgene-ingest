"""
Download cellxgene census data corresponding to observations in the index and push to s3
"""

import os
import tempfile
import argparse
import dotenv
import tqdm
import boto3
import pandas as pd

import cellxgene_census

dotenv.load_dotenv("defaults.env")
dotenv.load_dotenv(override=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-p", "--prefix", default="cellxgene", help="Path prefix in bucket"
    )
    parser.add_argument(
        "-l", "--limit", type=int, default=None, help="Limit to download"
    )
    parser.add_argument(
        "-c",
        "--chunk",
        type=int,
        default=1,
        help="Chunk size to include in each anndata file",
    )
    parser.add_argument("index", nargs="?", default="data/index.feather")
    args = parser.parse_args()

    df = pd.read_feather(args.index)

    print(f"Found {df.shape[0]:,} observations")

    # Subset if limit specified
    soma_ids = df.soma_joinid[0 : args.limit].values

    print(f"Downloading {len(soma_ids):,} observations")

    session = boto3.Session(profile_name=os.environ["S3_PROFILE"])
    s3 = session.client(
        service_name="s3",
        endpoint_url=os.environ["S3_ENDPOINT"],
    )

    with cellxgene_census.open_soma(
        census_version=os.environ["CENSUS_VERSION"]
    ) as census:
        for start_soma_id in tqdm.tqdm(range(0, len(soma_ids), args.chunk)):
            chunk = soma_ids[start_soma_id : start_soma_id + args.chunk]
            adata = cellxgene_census.get_anndata(
                census=census,
                organism="Homo sapiens",
                # var_value_filter="feature_id in ['ENSG00000161798', 'ENSG00000188229']",
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
                adata.write_h5ad(f.name)
                path = f"{args.prefix}/{str(chunk[0])}-{str(chunk[-1])}.h5ad"
                try:
                    response = s3.upload_file(f.name, os.environ["S3_BUCKET"], path)
                except boto3.ClientError as e:
                    print(e)
