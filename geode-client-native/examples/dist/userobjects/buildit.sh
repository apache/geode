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
  CC \
      -D_REENTRANT $OPT $ARCH \
      -I$GFCPP/include \
      -L$GFCPP/$LIBDIR \
      -R$GFCPP/$LIBDIR \
      *.cpp -o userobjects -lgfcppcache -lrt -lcollector -lkstat
elif [ "$platform" == "Linux" ]; then
  if [ $is64bit -eq 1 ]; then
    ARCH="-m64"
  else
    ARCH="-m32"
  fi
  g++ \
      -D_REENTRANT $OPT -Wall -Werror $ARCH \
      -I$GFCPP/include \
      -Xlinker -rpath -Xlinker $GFCPP/$LIBDIR -L$GFCPP/$LIBDIR \
      *.cpp -o userobjects -lgfcppcache
else
  echo "This script is not supported on this platform."
  exit 1
fi


