#!/usr/bin/env bash
set -x -e -o pipefail

curl -o /etc/yum.repos.d/slc6-scl.repo http://linuxsoft.cern.ch/cern/scl/slc6-scl.repo
yum -y --nogpgcheck install devtoolset-3-gcc devtoolset-3-gcc-c++

echo "source scl_source enable devtoolset-3" >> ~build/.bashrc
