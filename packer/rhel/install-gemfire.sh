#!/usr/bin/env bash
set -x -e -o pipefail

mkdir /gemfire
tar -zxf gemfire.tar.gz -C /gemfire
rm gemfire.tar.gz
