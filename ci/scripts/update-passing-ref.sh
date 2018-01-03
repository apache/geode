#!/usr/bin/env bash
export BUILDROOT=$(pwd)
REPOSITORY_DIR=$(pwd)/geode
LOCAL_FILE=${BUILDROOT}/results/passing.txt
DESTINATION_URL=gs://${PUBLIC_BUCKET}/passing.txt
pushd ${REPOSITORY_DIR}
git rev-parse HEAD > ${LOCAL_FILE}
popd
gsutil -q -m cp ${LOCAL_FILE} ${DESTINATION_URL}
