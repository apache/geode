#!/usr/bin/env bash
set -x -e -o pipefail

mkdir /gemfire
gtar -xzvf gemfire.tar.gz -C /gemfire
rm gemfire.tar.gz
