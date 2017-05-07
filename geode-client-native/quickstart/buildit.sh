#!/bin/bash
set -e

if [ -z ${GFCPP:-} ]; then
  echo GFCPP is not set.
  exit 1
fi
if [ -z ${GEMFIRE:-} ]; then
  echo GEMFIRE is not set.
  exit 1
fi

echo Building GemFire QuickStart examples...

OPT=-O3
LIBDIR=lib
platform=`uname`
platformArch=`uname -p`
if [ "$platform" = "SunOS" ]; then
    ARCH="-m64"
  CC=CC
  CXX_FLAGS="-mt -D_RWSTD_MULTI_THREAD -DTHREAD=MULTI \
    -D_REENTRANT $OPT $ARCH \
    -I$GFCPP/include"
    CXX_FLAGS="${CXX_FLAGS} -std=c++11"
  LD_FLAGS="-L$GFCPP/$LIBDIR \
    -R$GFCPP/$LIBDIR \
    -lgfcppcache -lrt -lpthread -lkstat"
elif [ "$platform" = "Linux" ]; then
    ARCH="-m64"
  CC=g++
  CXX_FLAGS="-D_REENTRANT $OPT -Wall $ARCH \
      -I$GFCPP/include"
  LD_FLAGS="-L$GFCPP/$LIBDIR \
      -Wl,-rpath,$GFCPP/$LIBDIR \
      -lgfcppcache"
else
  echo "This script is not supported on this platform."
  exit 1
fi

$GFCPP/bin/pdxautoserializer  --outDir=cpp/queryobjects  cpp/queryobjects/PortfolioPdxAuto.hpp 
$GFCPP/bin/pdxautoserializer  --outDir=cpp/queryobjects  cpp/queryobjects/PositionPdxAuto.hpp

$CC  $CXX_FLAGS \
    cpp/HACache.cpp -o cpp/HACache $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/Exceptions.cpp -o cpp/Exceptions $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/BasicOperations.cpp -o cpp/BasicOperations $LD_FLAGS 
$CC  $CXX_FLAGS \
    cpp/DistributedSystem.cpp -o cpp/DistributedSystem $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/RefIDExample.cpp -o cpp/RefIDExample $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/PoolWithEndpoints.cpp -o cpp/PoolWithEndpoints $LD_FLAGS 
$CC  $CXX_FLAGS \
    cpp/DataExpiration.cpp \
    cpp/plugins/SimpleCacheListener.cpp -o cpp/DataExpiration $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/LoaderListenerWriter.cpp \
    cpp/plugins/SimpleCacheLoader.cpp \
    cpp/plugins/SimpleCacheListener.cpp \
    cpp/plugins/SimpleCacheWriter.cpp -o cpp/LoaderListenerWriter $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/DurableClient.cpp \
    cpp/plugins/DurableCacheListener.cpp -o cpp/DurableClient $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/RegisterInterest.cpp -o cpp/RegisterInterest $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/Security.cpp -o cpp/Security $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/MultiuserSecurity.cpp -o cpp/MultiuserSecurity $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/RemoteQuery.cpp \
    cpp/queryobjects/Portfolio.cpp \
    cpp/queryobjects/Position.cpp -o cpp/RemoteQuery $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/PoolRemoteQuery.cpp \
    cpp/queryobjects/Portfolio.cpp \
    cpp/queryobjects/Position.cpp -o cpp/PoolRemoteQuery $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/CqQuery.cpp \
    cpp/queryobjects/Portfolio.cpp \
    cpp/queryobjects/Position.cpp -o cpp/CqQuery $LD_FLAGS

$CC  $CXX_FLAGS \
     cpp/PoolCqQuery.cpp \
     cpp/queryobjects/Portfolio.cpp \
     cpp/queryobjects/Position.cpp -o cpp/PoolCqQuery $LD_FLAGS

$CC  $CXX_FLAGS \
     cpp/Delta.cpp -o cpp/Delta $LD_FLAGS

$CC  $CXX_FLAGS \
    cpp/ExecuteFunctions.cpp  -o cpp/ExecuteFunctions $LD_FLAGS

$CC  $CXX_FLAGS \
    cpp/PoolCqQuery.cpp \
    cpp/queryobjects/Portfolio.cpp \
    cpp/queryobjects/Position.cpp -o  cpp/PoolCqQuery $LD_FLAGS
$CC  $CXX_FLAGS \
    interop/InteropCPP.cpp -o interop/InteropCPP $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/PutAllGetAllOperations.cpp -o cpp/PutAllGetAllOperations $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/Transactions.cpp -o cpp/Transactions $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/TransactionsXA.cpp -o cpp/TransactionsXA $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/PdxInstance.cpp -o cpp/PdxInstance $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/PdxSerializer.cpp -o cpp/PdxSerializer $LD_FLAGS
$CC  $CXX_FLAGS \
    cpp/PdxRemoteQuery.cpp \
    cpp/queryobjects/PortfolioPdx.cpp \
    cpp/queryobjects/PositionPdx.cpp -o cpp/PdxRemoteQuery $LD_FLAGS

$CC  $CXX_FLAGS \
    cpp/PdxAutoSerializer.cpp \
    cpp/queryobjects/PortfolioPdxAuto.cpp \
    cpp/queryobjects/PositionPdxAuto.cpp  \
    cpp/queryobjects/testobject_PortfolioPdxAutoSerializable.cpp \
    cpp/queryobjects/testobject_PositionPdxAutoSerializable.cpp -o cpp/PdxAutoSerializer $LD_FLAGS

javac -classpath $GEMFIRE/lib/gemfire.jar interop/InteropJAVA.java
