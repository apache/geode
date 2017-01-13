#!/usr/bin/env bash
set -x -e -o pipefail

tmp=`mktemp -d`

curl -o ${tmp}/jdk.rpm -v -j -k -L -H "Cookie: oraclelicense=accept-securebackup-cookie" \
    http://download.oracle.com/otn-pub/java/jdk/8u92-b14/jdk-8u92-linux-x64.rpm

yum install -y ${tmp}/jdk.rpm

rm -rf ${tmp}
