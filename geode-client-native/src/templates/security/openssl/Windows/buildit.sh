#!/bin/bash
openssl=openssl-0.9.8g
currentDir=`pwd`
cd `dirname $0`
install="`pwd`/ssllibs"
rm -rf $openssl $install
tar -zxf ${openssl}.tar.gz 2>&1 | tee build_$$.log
function doit () {
  cd $openssl
  ./config --prefix=$install --openssldir=$install/openssl threads zlib shared
#  make
#  make test
  make install
  cd ..
}

doit 2>&1 | tee build_$$.log
cd $currentDir
