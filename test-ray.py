import ray

ray.init(
    address="ray://127.0.0.1:10001",
    runtime_env={"pip": ["cellxgene-census==1.13.1"]},
    # runtime_env={
    #     "conda": {
    #         "channels": ["conda-forge"],
    #         "dependencies": ["requests", "tiledb-py"],
    #     },
    # },
)


@ray.remote
def f(x):
    # import requests
    # import tiledb
    import platform
    import cellxgene_census

    # print(requests.__version__)
    # print(tiledb.__version__)
    print(platform.platform())
    print(cellxgene_census.__version__)
    print("Hello from f!")
    return x * x


futures = [f.remote(i) for i in range(4)]
print(ray.get(futures))  # [0, 1, 4, 9]
