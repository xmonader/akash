#!/bin/sh
set -xe
ts=$(date '+%y-%m-%d-%H-%M-%S')
image_sha=$(docker build -f ../../_build/Dockerfile.akash ../../.cache/bin --quiet)

tag="ovrclk-local:${ts?}"

docker tag "${image_sha?}" "${tag?}"
exec echo "${tag?}" > $1
