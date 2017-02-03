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


#set -xv

#echo $1
GFCPP=`echo $1 | perl -p -e 's/\\\/\//g'`
#echo $GFCPP
INCDIR=$GFCPP/include
if [ ! -f "${INCDIR}/gfcpp/Region.hpp" ]; then
  echo "ERROR: Header files are not packed in product."
  exit 1
fi

HEADERS=`find $INCDIR -type d -name "impl" -prune -o -name "_*.hpp" \
  -o -name "*.hpp" -print` | grep -v PdxAutoSerializer

function compileHeaders {
  echo "0" >status
  if [ ! -z ${WINDIR:-} ]; then

    PRAGMAFILE=testHeaders_pragmas.hpp

    echo "#pragma warning (disable : 4514)   /* unreferenced inline */" \
      >$PRAGMAFILE
    echo "#pragma warning (disable : 4290)   /* throws ignored */" \
      >>$PRAGMAFILE

    COMPILE="cl /nologo /c /Zs /TP /EHsc /W3 /WX /FI$PRAGMAFILE -I$INCDIR -I. oneheader.cpp"
  elif [ `uname` == "SunOS" ]; then
    COMPILE="CC -c -I$INCDIR oneheader.cpp"
  else 
    COMPILE="g++ -c -I$INCDIR oneheader.cpp"
  fi

  for hpp in $HEADERS; do
    echo "#include <$hpp>" >oneheader.cpp
    $COMPILE || \
      ( failed=`cat status`; failed=`expr $failed + 1`; echo "$failed" >status; echo "ERROR in header $hpp" )
  done
}

compileHeaders 2>&1 | egrep -v "oneheader.cpp" | tee testHeaders.out
failed=`cat status`
outdir=`pwd`
echo "Discovered $failed headers with errors. See ${outdir}/testHeaders.out for details"
exit $failed
