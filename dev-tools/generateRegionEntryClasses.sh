#!/bin/bash
# Licensed to the Apache Software Foundation (ASF) under one or more contributor license
# agreements. See the NOTICE file distributed with this work for additional information regarding
# copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance with the License. You may obtain a
# copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under
# the License.
#
# This script should only be run from the top level build directory 
# (the one that contains the "dev-tools" subdirectory).
# It reads LeafRegionEntry.cpp, preprocesses it and generates all the leaf
# class source files that subclass AbstractRegionEntry.
# It executes cpp. It has been tested with gnu's cpp on linux.
# After using it to generate new java files, make sure and
# run './gradlew spotlessApply' to format these generated files.
# Then use 'git diff' to validate the changes you made to the
# generated java files before committing them.

SRCDIR=geode-core/src/main/java/org/apache/geode/internal/cache/entries
SRCFILE=$SRCDIR/LeafRegionEntry.cpp

for VERTYPE in VM Versioned
do
  for RETYPE in Thin Stats ThinLRU StatsLRU ThinDisk StatsDisk ThinDiskLRU StatsDiskLRU
  do
    for KEY_INFO in 'ObjectKey KEY_OBJECT' 'IntKey KEY_INT' 'LongKey KEY_LONG' 'UUIDKey KEY_UUID' 'StringKey1 KEY_STRING1' 'StringKey2 KEY_STRING2'
    do
      for MEMTYPE in Heap OffHeap
      do
      declare -a KEY_ARRAY=($KEY_INFO)
      KEY_CLASS=${KEY_ARRAY[0]}
      KEY_TYPE=${KEY_ARRAY[1]}
      BASE=${VERTYPE}${RETYPE}RegionEntry${MEMTYPE}
      OUT=${BASE}${KEY_CLASS}
      WP_ARGS=-Wp,-C,-P,-D${KEY_TYPE},-DPARENT_CLASS=$BASE,-DLEAF_CLASS=$OUT
      if [ "$VERTYPE" = "Versioned" ]; then
        WP_ARGS=${WP_ARGS},-DVERSIONED
      fi
      if [[ "$RETYPE" = *Stats* ]]; then
        WP_ARGS=${WP_ARGS},-DSTATS
      fi
      if [[ "$RETYPE" = *Disk* ]]; then
        WP_ARGS=${WP_ARGS},-DDISK
      fi
      if [[ "$RETYPE" = *LRU* ]]; then
        WP_ARGS=${WP_ARGS},-DLRU
      fi
      if [[ "$MEMTYPE" = "OffHeap" ]]; then
        WP_ARGS=${WP_ARGS},-DOFFHEAP
      fi
      echo generating $SRCDIR/$OUT.java
      cpp -E $WP_ARGS $SRCFILE >$SRCDIR/$OUT.java
      #echo VERTYPE=$VERTYPE RETYPE=$RETYPE $KEY_INFO KEY_CLASS=$KEY_CLASS KEY_TYPE=$KEY_TYPE args=$WP_ARGS 
      done
    done
  done
done
echo now run \'./gradlew spotlessApply\' to format the generated files
