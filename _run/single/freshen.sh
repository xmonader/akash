#!/bin/bash

set -xe

pushd ../../
make all
popd
make kustomize-init-docker-image
make kind-configure-image
make kind-upload-image

make kustomize-install-provider
make kustomize-install-ip-operator
make kustomize-install-hostname-operator

