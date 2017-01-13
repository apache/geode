#!/bin/bash


if [ -z ${GFCPP:-} ]; then
  echo set GFCPP...
  exit 1
fi

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
  CC  \
      -mt -D_RWSTD_MULTI_THREAD -DTHREAD=MULTI \
      -D_REENTRANT -D_EXAMPLE $OPT $ARCH \
      -I$GFCPP/include \
      -L$GFCPP/$LIBDIR \
      -R$GFCPP/$LIBDIR \
      -lgfcppcache -lrt -lpthread -lkstat \
      CacheRunner.cpp CommandReader.cpp Test*.cpp Po*.cpp -o cacheRunner 
elif [ "$platform" == "Linux" ]; then
  if [ $is64bit -eq 1 ]; then
    ARCH="-m64"
  else
    ARCH="-m32"
  fi
  g++ \
      -D_REENTRANT -D_EXAMPLE $OPT -Wall $ARCH \
      -I$GFCPP/include \
      -Wl,-rpath,$GFCPP/$LIBDIR -L$GFCPP/$LIBDIR -lgfcppcache \
      CacheRunner.cpp CommandReader.cpp Test*.cpp Po*.cpp -o cacheRunner 
else
  echo "This script is not supported on this platform."
  exit 1
fi
