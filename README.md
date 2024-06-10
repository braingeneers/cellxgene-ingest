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

# Performance
```
$ python ingest-pool.py -n 10000 -c 100 -d 20 personal/rcurrie/cellxgene
Downloading 10,000 observations in 100 files to s3://braingeneers/personal/rcurrie/cellxgene/
2024-06-10 06:40:08,670 INFO worker.py:1740 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265 
Creating pool of 20 ray actors...
Ingesting...
100%|█████████████████████████████████████████████████████████| 100/100 [02:02<00:00,  1.22s/it]
Done.
100 files ingested in 2.04 minutes.
3.40 hours per 1M observations.
11.19 MB average file size.
```

```
$ python ingest-pool.py -n 10000 -c 100 -d 40 personal/rcurrie/cellxgene
Downloading 10,000 observations in 100 files to s3://braingeneers/personal/rcurrie/cellxgene/
2024-06-10 06:44:03,905 INFO worker.py:1740 -- Started a local Ray instance. View the dashboard at 127.0.0.1:8265 
Creating pool of 40 ray actors...
Ingesting...
100%|█████████████████████████████████████████████████████████| 100/100 [01:24<00:00,  1.19it/s]
Done.
100 files ingested in 1.41 minutes.
2.34 hours per 1M observations.
11.19 MB average file size.
```

## TileDB-SOMA On Older CPUs
The cellxgene-census package depends on [TileDB-SOMA](https://github.com/single-cell-data/TileDB-SOMA) which leverages AVX2 on modern CPUs. TileDB-SOMA python wheels assume AVX2 generating an ```illegal hardware instruction (core dumped)``` on CPUs without AVX2 (```cat /proc/cpuinfo | grep avx2 } head -1```). To run on non-AVX2 cpus build from source and install into your existing python environment or active virtualenv via:
```
git clone https://github.com/single-cell-data/TileDB-SOMA.git
pip install -v -e TileDB-SOMA/apis/python
```

# References
[Cell x Gene](https://cellxgene.cziscience.com)

[TileDB 101: Single Cell](https://tiledb.com/blog/tiledb-101-single-cell)

[anndata - Annotated data](https://anndata.readthedocs.io)

[Python and boto3 Performance Adventures: Synchronous vs Asynchronous AWS API Interaction](https://joelmccoy.medium.com/python-and-boto3-performance-adventures-synchronous-vs-asynchronous-aws-api-interaction-22f625ec6909)

## Ray
For local development get a [Ray Cluster Running](https://docs.ray.io/en/latest/cluster/kubernetes/getting-started/raycluster-quick-start.html#kuberay-raycluster-quickstart)

[Interactive Ray Service development](https://docs.ray.io/en/latest/cluster/running-applications/job-submission/ray-client.html)

For details on using Ray Actors vs. Data for processing see [Model Batch Inference in Ray: Actors, ActorPool, and Datasets](https://www.anyscale.com/blog/model-batch-inference-in-ray-actors-actorpool-and-datasets)