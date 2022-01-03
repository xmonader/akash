#!/bin/bash

make clean
make kind-cluster-delete

set -xe
make kind-cluster-create

make init
make kustomize-init

make kustomize-init-docker-image
make kind-configure-image
make kind-upload-image

sleep 65 # waiting on the ingress-nginx to be ready. Need a better way to do this

make kustomize-install-node

sleep 6

make provider-create

sleep 1
make kustomize-install-ip-operator
make kustomize-install-hostname-operator


make kustomize-install-provider
