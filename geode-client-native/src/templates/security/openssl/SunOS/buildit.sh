#!/bin/bash
openssl=openssl-0.9.8g
currentDir=`pwd`
cd `dirname $0`
install="`pwd`/ssllibs"
rm -rf $openssl $install
gtar -zxf ${openssl}.tar.gz 2>&1

if [ `uname`  == "Linux" ]; then 
  function doit () {
    cd $openssl
    ./config --prefix=$install --openssldir=$install/openssl threads zlib shared
  #  make
  #  make test
    make install
    cd ..
  }
else
  function doit () {
    export PATH=/export/std11_gfe/bin:/usr/ccs/bin:$PATH
    cd $openssl
    ./Configure solaris-sparcv8-cc --prefix=$install --openssldir=$install/openssl threads zlib shared
  #  make
  #  make test
    make install
    cd ..
  }
fi

doit 2>&1 | tee build_$$.log
cd $currentDir
