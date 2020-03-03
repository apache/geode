#!/usr/bin/env bash

while getopts ":b:c:" opt; do
  case ${opt} in
  b)
    BASELINE_COMMIT=${OPTARG}
    ;;
  c)
    COMPARISON_COMMIT=${OPTARG}
    ;;
  \?)
    echo "Usage: ${0} -b BASELINE_COMMIT -c COMPARISON_COMMIT"
    exit 0
    ;;
  :)
    echo "Invalid option: $OPTARG requires an argument" 1>&2
    exit 1
    ;;
  esac
done

if [ -z ${BASELINE_COMMIT} ] || [ -z ${COMPARISON_COMMIT} ]; then
  echo "Must specify both BASELINE_COMMIT and COMPARISON_COMMIT. Shame on you."
  exit 1
fi

echo "BASELINE_COMMIT: ${BASELINE_COMMIT}"
echo "COMPARISON_COMMIT: ${COMPARISON_COMMIT}"

ORIGINAL_BRANCH=$(git branch | sed -n -e 's/^\* \(.*\)/\1/p')
echo " ORIGINAL_BRANCH: ${ORIGINAL_BRANCH}"

git checkout ${BASELINE_COMMIT}
RETURN_CODE=$?
 if [[ ${RETURN_CODE} -ne 0 ]] ; then
  echo "Please stash any uncommitted changes before using this script."
  exit 1
fi

bash benchmark.sh -g

git checkout ${COMPARISON_COMMIT}

bash benchmark.sh -g

git checkout ${ORIGINAL_BRANCH}
