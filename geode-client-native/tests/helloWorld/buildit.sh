#!/bin/bash


if [ -z ${GFCPP:-} ]; then
  echo set GFCPP...
  exit 1
fi

OPT=-O3
LIBDIR=lib

g++ \
    -D_REENTRANT $OPT -Wall -Werror -m32\
    -I$GFCPP/include \
    -Xlinker -rpath -Xlinker $GFCPP/$LIBDIR -L$GFCPP/$LIBDIR \
    *.cpp -o gfHellWorld -lgfcppcache


