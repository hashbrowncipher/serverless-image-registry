#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail
set -x

mkdir -p blobs manifests

TOKEN=$(curl -G "https://auth.docker.io/token" \
	-d service=registry.docker.io \
	-d scope=repository:library/ubuntu:pull | jq -r ".token"
)

function registry_get {
  curl -L -H "Authorization: Bearer $TOKEN" "$@"

}

registry_get \
  -H"Accept: application/vnd.docker.distribution.manifest.v2+json" \
  https://index.docker.io/v2/library/ubuntu/manifests/focal > manifests/ubuntu:focal

CONFIG=$(jq -r ".config.digest" manifests/ubuntu:focal)

registry_get https://index.docker.io/v2/library/ubuntu/blobs/$CONFIG > blobs/$CONFIG

jq -r ".layers[].digest" manifests/ubuntu:focal | while read line; do
  registry_get "https://index.docker.io/v2/library/ubuntu/blobs/$line" > "blobs/$line"
done
