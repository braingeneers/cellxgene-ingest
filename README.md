# cellxgene-ingest
Ingest cellxgene data into s3 as chunked h5ad files

# Run
Query cellxgene for a list of sample ids and save locally as a feather file:
```
python index.py
```
You can then further filter this index based on associated metadata:
```
In [3]: df = pd.read_feather("data/index.feather")
In [4]: df.head()
Out[4]: 
   soma_joinid      assay cell_type tissue tissue_general suspension_type disease
0      7560043  10x 3' v2    T cell  blood          blood            cell  normal
1      7560044  10x 3' v2    T cell  blood          blood            cell  normal
```

Download from cellxgene 2 genes from 1 observation and upload to braingeneers/personal/foo
```
python ingest.py -n 1 -c 1 -d 1 --gene-filter ENSG00000161798,ENSG00000139618 personal/foo
```

# Install

```
pip install -r requirements.txt
```

## TileDB-SOMA On Older CPUs
The cellxgene-census package depends on [TileDB-SOMA](https://github.com/single-cell-data/TileDB-SOMA) which leverages AVX2 on modern CPUs. TileDB-SOMA python wheels assume AVX2 generating an ```illegal hardware instruction (core dumped)``` on CPUs without AVX2 (```cat /proc/cpuinfo | grep avx2 } head -1```). To run on non-AVX2 cpus build from source and install into your existing python environment or active virtualenv via:
```
git clone https://github.com/single-cell-data/TileDB-SOMA.git
pip install -v -e TileDB-SOMA/apis/python
```

## Ray
Note: Not yet tested running in a remote Ray cluster, only local

For local development get a [Ray Cluster Running](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html#kuberay-raycluster-quickstart)

[Interactive Ray Service development](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/ray-client.html)


# References
[Cell x Gene](https://cellxgene.cziscience.com)

[TileDB 101: Single Cell](https://tiledb.com/blog/tiledb-101-single-cell)

[anndata - Annotated data](https://anndata.readthedocs.io)