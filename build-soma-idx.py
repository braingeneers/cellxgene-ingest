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
        obs_value_filter="sex == 'female' and cell_type in ['microglial cell', 'neuron']",
        column_names={
            "obs": [
                "assay",
                "cell_type",
                "tissue",
                "tissue_general",
                "suspension_type",
                "disease",
            ]
        },
    )

    print(adata)
