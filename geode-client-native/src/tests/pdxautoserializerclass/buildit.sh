#!/bin/bash
#set -x
if [ -z ${GFCPP:-} ]; then
  echo GFCPP is not set.
  exit 1
fi

echo Generating Pdx classes...
#hppDir=$1
#cd $hppDir
#echo $PWD
for fileName in *.hpp
do
  echo "filename = $fileName"
 [[ -f "$fileName" ]] || continue
 echo $GFCPP/bin/pdxautoserializer --outDir=. $fileName
 $GFCPP/bin/pdxautoserializer --outDir=. $fileName
done

$GFCPP/bin/pdxautoserializer --outDir=. --classNameStr=AutoPdxVersioned1:PdxTests.PdxVersioned AutoPdxVersioned1.hpp
$GFCPP/bin/pdxautoserializer --outDir=. --classNameStr=AutoPdxVersioned2:PdxTests.PdxVersioned AutoPdxVersioned2.hpp
echo Compiling Pdx generated class

LIBDIR=lib

platform=`uname`
is64bit=__IS_64_BIT__
if [ "$platform" == "SunOS" ]; then
  if [ $is64bit -eq 1 ]; then
    ARCH="-m64"
  else
    ARCH="-m32"
  fi
  SLOW_OPTIONS="-g -O0 -DDEBUG=1 -DASSERT_LEVEL=4"
  FAST_OPTIONS="-O3 -DNDEBUG=1 -DASSERT_LEVEL=1"
  CC  -G -Bdynamic \
      -mt -D_RWSTD_MULTI_THREAD -DTHREAD=MULTI \
      -D_REENTRANT -D_SOLARIS $FAST_OPTIONS $ARCH \
      -I$GFCPP/include \
      -I${basedir}/tests/fwklib -I${basedir}/tests \
      -L$GFCPP/$LIBDIR \
      -lgfcppcache -lpthread \
      *.cpp -o ${OSBUILDDIR}/framework/lib/libpdxobject.so
  exitStatus=$?
  CC  -G -Bdynamic \
      -mt -D_RWSTD_MULTI_THREAD -DTHREAD=MULTI \
      -D_REENTRANT -D_SOLARIS $SLOW_OPTIONS $ARCH \
      -I$GFCPP/include \
      -I${basedir}/tests/fwklib -I${basedir}/tests \
      -L$GFCPP/$LIBDIR \
      -lgfcppcache -lpthread \
      *.cpp -o ${OSBUILDDIR}/framework/lib/debug/libpdxobject.so
  exitStatus=$?
elif [ "$platform" == "Linux" ]; then
  if [ $is64bit -eq 1 ]; then
    ARCH="-m64"
    FPIC="-fPIC"
  else
    ARCH="-m32"
    FPIC=""
  fi
  SLOW_OPTIONS="-g -O0 -DDEBUG=1 -DASSERT_LEVEL=4 -fno-inline"
  FAST_OPTIONS="-O3 -DNDEBUG=1 -DASSERT_LEVEL=1"
  gcc -shared \
      -D_REENTRANT $FAST_OPTIONS -Wall $FPIC $ARCH $@\
      -I$GFCPP/include \
      -I${basedir}/tests/fwklib -I${basedir}/tests \
      -L${GFCPP}/${LIBDIR} \
      -lgfcppcache \
      *.cpp -o ${OSBUILDDIR}/framework/lib/libpdxobject.so
 exitStatus=$?
  gcc -shared \
      -D_REENTRANT $SLOW_OPTIONS -Wall $FPIC $ARCH $@\
      -I$GFCPP/include \
      -I${basedir}/tests/fwklib -I${basedir}/tests \
      -L${GFCPP}/${LIBDIR} -Wl,-rpath \
      -lgfcppcache \
      *.cpp -o ${OSBUILDDIR}/framework/lib/debug/libpdxobject.so
 exitStatus=$?
else
  echo "This script is not supported on this platform."
  exit 1
fi
echo build complete.
exit ${exitStatus}

#for cppfileName in *.cpp
#do
#SUBSTRING=`echo ${cppfileName}| cut -d'.' -f 1`
#$CC  $CXX_FLAGS \
#    -c -o $GFCPP/../tests/objects/testobject/$SUBSTRING.o $cppfileName
#done
