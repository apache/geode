#!/usr/bin/env bash
#
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

set -e 

if ! [ -d dev-tools ] ; then
  echo "Please run $0 from the top-level of your geode checkout"
  exit 1
fi

if [ "$1" = "-l" ] ; then
  ./gradlew dependencyUpdates; find . | grep build/dependencyUpdates/report.txt | xargs cat \
   | grep ' -> ' | egrep -v '(Gradle|antlr|protobuf|lucene|JUnitParams|docker-compose-rule|javax.servlet-api|gradle-tooling-api|springfox)' \
   | sort -u | tr -d '][' | sed -e 's/ -> / /' -e 's#.*:#'"$0"' #'
  exit 0
fi

if [ "$3" = "" ] ; then
  echo "Usage: $0 <library-name> <old-ver> <new-ver>"
  echo "   or: $0 -l"
  exit 1
fi

if [ $(git diff | wc -l) -gt 0 ] ; then
  echo "Your workspace has uncommitted changes, please stash them."
  exit 1
fi

NAME="$1"
SRCH="$2"
REPL="$3"
SRCH=${SRCH//./\\.}
git grep -n --color "$SRCH" | cat
git grep -l "$SRCH" | while read f; do
  sed -e "s/$SRCH/$REPL/g" -i.bak $f || true
  rm -f $f.bak
done
git add -p
git commit -m "Bump $NAME from $OLDV to $NEWV"
if [ $(git diff | wc -l) -gt 0 ] ; then
  git stash
  git stash drop
fi
./gradlew devBuild
