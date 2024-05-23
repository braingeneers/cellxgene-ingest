import cellxgene_census

VERSION = "2023-12-15"

with cellxgene_census.open_soma(census_version=VERSION) as census:

    cell_metadata = census["census_data"]["homo_sapiens"].obs.read(
        value_filter="disease=='normal' and suspension_type=='cell' and tissue in ['heart', 'blood', 'brain', 'lung', 'kidney', 'intestine', 'pancreas']",
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

# Concatenates results to pyarrow.Table
dt = cell_metadata.concat()

# Converts to pandas.DataFrame
df = dt.to_pandas()

print(df.shape)

soma_ids = df.soma_joinid[0:10].values

with cellxgene_census.open_soma(census_version=VERSION) as census:
    adata = cellxgene_census.get_anndata(
        census=census,
        organism="Homo sapiens",
        var_value_filter="feature_id in ['ENSG00000161798', 'ENSG00000188229']",
        obs_coords=soma_ids,
        column_names={
            "obs": [
                "soma_joinid",
                "assay",
                "cell_type",
                "tissue",
                "tissue_general",
                "suspension_type",
                "disease",
            ],
            "var": ["soma_joinid", "feature_id", "feature_name", "feature_length"],
        },
    )

print(adata.to_df())
adata.write_h5ad("test.h5ad")

# import boto3
# import smart_open

# client = boto3.client("s3", endpoint_url="https://s3-west.nrp-nautilus.io")
# tparams = {"client": client}

# with smart_open.open(
#     "s3://braingeneers/cellxgene/test.h5ad", "w", transport_params=tparams) as f:
#     adata.write_h5ad(f)


# import anndata as ad
# import h5py
# import remfile

# ADATA_URI = "https://allen-brain-cell-atlas.s3.us-west-2.amazonaws.com/expression_matrices/WMB-10Xv2/20230630/WMB-10Xv2-TH-log2.h5ad"

# file_h5 = h5py.File(remfile.File(ADATA_URI), "r")

# # Read the whole file
# adata = ad.experimental.read_elem(file_h5)
