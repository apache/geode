#!/usr/bin/env bash
set -x -e -o pipefail

tmp=`mktemp`

curl -o ${tmp} -v -L https://cmake.org/files/v3.5/cmake-3.5.0-Linux-x86_64.sh

bash ${tmp} --skip-license --prefix=/usr/local

rm -f ${tmp}
