#! /bin/bash
#=========================================================================
# Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
# This product is protected by U.S. and international copyright
# and intellectual property laws. Pivotal products are covered by
# more patents listed at http://www.pivotal.io/patents.
#========================================================================

GENERATED_DIR=$1
INTERNAL_SRC_DIR=$2

rm -f $GENERATED_DIR/SolarisMapFile.*

cat $INTERNAL_SRC_DIR/SolarisMapFile_global.mak > $GENERATED_DIR/SolarisMapFile.map

grep -h JNIEXPORT $GENERATED_DIR/*.h > $GENERATED_DIR/SolarisMapFile.jni

sed -e '1,$s+JNIEXPORT.*JNICALL++' < $GENERATED_DIR/SolarisMapFile.jni > $GENERATED_DIR/SolarisMapFile.jni2

sed -e '1,$s+$+;+' < $GENERATED_DIR/SolarisMapFile.jni2 >> $GENERATED_DIR/SolarisMapFile.map

cat $INTERNAL_SRC_DIR/SolarisMapFile_local.mak >> $GENERATED_DIR/SolarisMapFile.map

