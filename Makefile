install-distributed-osx:
	brew install --cask docker
	brew install kubectl helm kind

shell-into-pod:
	# Replace name of pod with that from kubectl get pods
	kubectl exec --stdin --tty <name-of-pod> -- /bin/bash

forward-ray-dashboard:
	kubectl port-forward service/raycluster-kuberay-head-svc 8265:8265
	
forward-ray-head:
	kubectl port-forward service/raycluster-kuberay-head-svc 10001:10001

open-ray-dashboard:
	open http://127.0.0.1:8265


ray-kind-cluster-up-osx:
	# NRP is at v1.26.11 as of 05-18-2024
	kind create cluster --image=kindest/node:v1.26.15
	kubectl version

	helm repo add kuberay https://ray-project.github.io/kuberay-helm/
	helm repo update

	helm install kuberay-operator kuberay/kuberay-operator --version 1.1.1
	kubectl get pods

	helm install raycluster kuberay/ray-cluster --version 1.1.1 --set 'image.tag=2.22.0-py311-aarch64'


	helm install raycluster kuberay/ray-cluster --version 1.1.1 --set 'image.tag=2.22.0-py311'

	kubectl get pods
	# kubectl get rayclusters
	# kubectl get pods --selector=ray.io/cluster=raycluster-kuberay

ray-kind-cluster-down:
	helm uninstall raycluster
	# release "raycluster" uninstalled

	kubectl get pods
	# NAME                                READY   STATUS    RESTARTS   AGE
	# kuberay-operator-7fbdbf8c89-pt8bk   1/1     Running   0          XXm

	# Uninstall the KubeRay operator Helm chart
	helm uninstall kuberay-operator
	# release "kuberay-operator" uninstalled

	# Confirm that the KubeRay operator pod is gone by running
	kubectl get pods
	# No resources found in default namespace.

	# [Step 5.3]: Delete the Kubernetes cluster
	kind delete cluster

download-gene-list:
	# https://www.gsea-msigdb.org/gsea/msigdb/cards/KEGG_O_GLYCAN_BIOSYNTHESIS
	wget -O genes.json "https://www.gsea-msigdb.org/gsea/msigdb/human/download_geneset.jsp?geneSetName=KEGG_O_GLYCAN_BIOSYNTHESIS&fileType=json"

	# WNT Signaling Pathway
	wget -O genes.json "https://www.gsea-msigdb.org/gsea/msigdb/human/download_geneset.jsp?geneSetName=BIOCARTA_WNT_PATHWAY&fileType=json"


	# Mapping between HUGO and ENSEMBLE
	wget -N "https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_46/gencode.v46.annotation.gtf.gz"

ls-braingeneers-s3-bucket:
	aws --endpoint https://s3-west.nrp-nautilus.io s3 ls --no-verify-ssl  s3://braingeneers

push-to-s3:
	aws --endpoint https://s3-west.nrp-nautilus.io s3 sync --no-verify-ssl data/100x100/  s3://braingeneers/cellxgene/ --delete