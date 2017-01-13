#!/usr/bin/env bash
set -x -e -o pipefail

yum install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-6.noarch.rpm
yum install -y https://rhel6.iuscommunity.org/ius-release.rpm

