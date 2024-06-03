# cellxgene-ingest

# Install

## TileDB-SOMA On Older CPUs
The cellxgene-census package depends on [TileDB-SOMA](https://github.com/single-cell-data/TileDB-SOMA) which leverages AVX2 on modern CPUs. TileDB-SOMA python wheels assume AVX2 generating an ```illegal hardware instruction (core dumped)``` on CPUs without AVX2 (```cat /proc/cpuinfo | grep avx2 } head -1```). To run on non-AVX2 cpus build from source and install into your existing python environment or active virtualenv via:
```
git clone https://github.com/single-cell-data/TileDB-SOMA.git
pip install -v -e TileDB-SOMA/apis/python
```

## Ray
For local development get a [Ray Cluster Running](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html#kuberay-raycluster-quickstart)

[Interactive Ray Service development](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/ray-client.html)

# References

[TileDB 101: Single Cell](https://tiledb.com/blog/tiledb-101-single-cell)

[anndata - Annotated data](https://anndata.readthedocs.io/en/latest/)