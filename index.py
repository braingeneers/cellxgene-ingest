"""
Generate an index of observations with meta data by querying cellxgene
"""

import os
import argparse
import dotenv

import cellxgene_census

dotenv.load_dotenv("defaults.env")
dotenv.load_dotenv(override=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-o", "--output", default="data/index.feather", help="Output index feather file"
    )
    args = parser.parse_args()

    print("Querying cellxgene census using value_filter:\n", os.environ["VALUE_FILTER"])

    with cellxgene_census.open_soma(
        census_version=os.environ["CENSUS_VERSION"]
    ) as census:
        cell_metadata = census["census_data"]["homo_sapiens"].obs.read(
            value_filter=os.environ["VALUE_FILTER"],
            column_names=[
                "soma_joinid",
                "assay",
                "cell_type",
                "tissue",
                "tissue_general",
                "suspension_type",
                "disease",
            ],
        )

    df = cell_metadata.concat().to_pandas()

    print(f"Found {df.shape[0]:,} observations")

    # Convert to categorical to save space
    cols = [
        "assay",
        "cell_type",
        "tissue",
        "tissue_general",
        "suspension_type",
        "disease",
    ]
    df[cols] = df[cols].astype("category")

    df.to_feather(args.output)
    print(f"Saved locally as {args.output}")
