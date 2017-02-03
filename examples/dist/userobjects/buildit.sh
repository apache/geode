#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



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
      *.cpp -o userobjects -lapache-geode -lrt -lcollector -lkstat
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
      *.cpp -o userobjects -lapache-geode
else
  echo "This script is not supported on this platform."
  exit 1
fi


