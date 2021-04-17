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

if [ "$2" = "-l" ] ; then
  ./gradlew dependencyUpdates -Drevision=release ; find . | grep build/dependencyUpdates/report.txt | xargs cat \
   | grep ' -> ' | egrep -v '(Gradle|antlr|lucene|JUnitParams|docker-compose-rule|javax.servlet-api|springfox|derby|selenium|jgroups|\[6.0.37|commons-collections|jaxb|testcontainers)' \
   | sort -u | tr -d '][' | sed -e 's/ -> / /' -e 's#.*:#'"$0 $1"' #'
  exit 0
fi

if [ "$4" = "" ] ; then
  echo "Usage: $0 <jira> <library-name> <old-ver> <new-ver>"
  echo "   or: $0 <jira> -l"
  exit 1
fi

if [ $(git diff | wc -l) -gt 0 ] ; then
  echo "Your workspace has uncommitted changes, please stash or commit them."
  exit 1
fi

JIRA="$1"
NAME="$2"
SRCH="$3"
REPL="$4"
OLDV="$SRCH"
SRCH=${SRCH//./\\.}
git grep -n --color "$SRCH" | cat
git grep -l "$SRCH" | while read f; do
  sed -e "s/$SRCH/$REPL/g" -i.bak $f || true
  rm -f $f.bak
done
git add -p
git commit -m "${JIRA}: Bump $NAME from $OLDV to $REPL"
if [ $(git diff | wc -l) -gt 0 ] ; then
  git stash
  git stash drop
fi
[ -n "$SKIP" ] || ./gradlew devBuild checkPom :geode-assembly:integrationTest --tests AssemblyContentsIntegrationTest --tests GeodeDependencyJarIntegrationTest --tests BundledJarsJUnitTest --tests GemfireCoreClasspathTest
