#!/bin/sh

set -ex

IMAGE=quay.io/acoustid/acoustid-index

if [ -n "$CI_COMMIT_TAG" ]
then
  VERSION=$(echo "$CI_COMMIT_TAG" | sed 's/^v//')
else
  VERSION=$CI_COMMIT_REF_SLUG
fi

docker build -t $IMAGE:$VERSION ci/
docker push $IMAGE:$VERSION

if [ -n "$CI_COMMIT_TAG" ]
then
    docker tag $IMAGE:$VERSION $IMAGE:latest
    docker push $IMAGE:latest
    docker rmi $IMAGE:latest
fi

docker rmi $IMAGE:$VERSION
