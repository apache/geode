#!/bin/bash


if [ -z ${GFCPP:-} ]; then
  echo GFCPP is not set.
  exit 1
fi

echo Building GemFire ExecuteFunction

OPT=-O3
LIBDIR=lib

platform=`uname`
is64bit=__IS_64_BIT__
if [ "$platform" == "SunOS" ]; then
  if [ $is64bit -eq 1 ]; then
    ARCH="-xarch=v9"
  else
    ARCH="-xarch=v8plus"
  fi
  CC=CC
  CXX_FLAGS="-mt -D_RWSTD_MULTI_THREAD -DTHREAD=MULTI \
      -D_REENTRANT $OPT $ARCH \
      -I$GFCPP/include \
      -L$GFCPP/$LIBDIR \
      -R$GFCPP/$LIBDIR \
      -lgfcppcache -lrt -lpthread -lkstat"
elif [ "$platform" == "Linux" ]; then
  if [ $is64bit -eq 1 ]; then
    ARCH="-m64"
  else
    ARCH="-m32"
  fi
  CC=g++
  CXX_FLAGS="-D_REENTRANT $OPT -Wall $ARCH \
      -I$GFCPP/include \
      -Xlinker -rpath -Xlinker $GFCPP/$LIBDIR -L$GFCPP/$LIBDIR \
      -lgfcppcache"
else
  echo "This script is not supported on this platform."
  exit 1
fi

$CC  $CXX_FLAGS \
    ExecuteFunctions.cpp -o ExecuteFunctions 
