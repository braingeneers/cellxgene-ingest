"""
Generate an index of observations with meta data by querying cellxgene
"""

import argparse
import cellxgene_census

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--census-version", default="2023-12-15")
    parser.add_argument(
        "--value-filter",
        # default="tissue in ['heart', 'blood', 'brain', 'lung', 'kidney', 'intestine', 'pancreas']",
        default="disease=='normal' and tissue in ['heart', 'blood', 'brain', 'lung', 'kidney', 'intestine', 'pancreas']",
        # Note: excluding nucleus suspension excludes most brain datasets...
        # default="disease=='normal' and suspension_type=='cell' and tissue in ['heart', 'blood', 'brain', 'lung', 'kidney', 'intestine', 'pancreas']",
    )
    parser.add_argument("output", nargs="?", default="index.feather")
    args = parser.parse_args()

    print(
        f"Querying census {args.census_version} with value_filter:\n{args.value_filter}"
    )

    with cellxgene_census.open_soma(census_version=args.census_version) as census:
        cell_metadata = census["census_data"]["homo_sapiens"].obs.read(
            value_filter=args.value_filter,
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
