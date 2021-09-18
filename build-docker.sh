#!/usr/bin/env bash
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

# Build the installer archive, including the Java JAR file and all dependencies.
set -ex
ROOT_DIR=$(readlink -f $(dirname $0)/)
DOCKER_REGISTRY=${DOCKER_REGISTRY:-devops-repo.isus.emc.com:8116/nautilus/mqtt-stresser}
DOCKER_TAG=${DOCKER_TAG:-latest}

# Create, push, and save Docker image.
pushd ${ROOT_DIR}
docker build -t ${DOCKER_REGISTRY}:${DOCKER_TAG} . 
docker push ${DOCKER_REGISTRY}:${DOCKER_TAG}
