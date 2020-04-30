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

usage() {
    echo "Usage: license_review.sh -v version_number_or_tgz [-p previous_version_number_or_tgz]"
    echo "  -v   The #.#.#.RC# or #.#.# version number to review -or- a path or URL to .tgz -or- 'HEAD'"
    echo "  -p   The #.#.#.RC# or #.#.# version number to compare against -or- a path or URL to .tgz"
    echo "  -n   No license check (useful if you just want the version comparison)"
    exit 1
}


while getopts ":v:p:n" opt; do
  case ${opt} in
    v )
      NEW_VERSION=$OPTARG
      ;;
    p )
      OLD_VERSION=$OPTARG
      ;;
    n )
      SKIP_LICENSES=true
      ;;
    \? )
      usage
      ;;
  esac
done


WORKSPACE=$(PWD)/license_tmp
DOWNLOAD=${WORKSPACE}/download
EXTRACT=${WORKSPACE}/extracted
mkdir -p ${DOWNLOAD}
mkdir -p ${EXTRACT}
[ "$NEW_VERSION" = "HEAD" ] && licFromWs=true
root=$0
root=${root%/dev-tools*}


function resolve() {
  [ -n "$1" ] || return
  spec=$1
  if [ "HEAD" = "$spec" ] ; then
    (cd $root && ./gradlew distTar 1>&2)
    spec=$root/geode-assembly/build/distributions/$(cd $root/geode-assembly/build/distributions && ls -t | grep apache-geode-.*-SNAPSHOT.tgz | tail -1)
    [ -r "$spec" ] || echo "Build not found: $spec" 1>&2
    [ -r "$spec" ]
  fi

  if [[ $spec =~ ^([0-9]+\.[0-9]+\.[0-9]+)\.(RC[0-9]+)$ ]]; then
    mmp=$(echo $spec | sed 's/.RC.*//')
    #bare RC version -> RC url
    spec=https://dist.apache.org/repos/dist/dev/geode/${spec}/apache-geode-${mmp}.tgz
  elif [[ $spec =~ ^([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    #bare released version -> release url
    spec=https://downloads.apache.org/geode/${spec}/apache-geode-${spec}.tgz
  elif echo "$spec" | grep -q '^http.*tgz$' ; then
    #tgz url
    true
  elif [ -r "$spec" ] && echo "$spec" | grep -q 'tgz$' ; then
    #tgz file present locally
    true
  else
    #unsupported
    return
  fi
  
  #download if url (and not already downloaded)
  if echo "$spec" | grep -q '^http.*tgz$' ; then
    filename=$(echo $spec | sed 's#.*/##')
    [ -r ${DOWNLOAD}/$filename ] || curl -L "$spec" > ${DOWNLOAD}/$filename
    spec=${DOWNLOAD}/$filename
  fi

  #extract it (if not already extracted)
  dirname=$(echo $spec | sed -e 's#.*/##' -e 's#.tgz$##')
  [ "${licFromWs}" = "true" ] && rm -Rf ${EXTRACT}/$dirname
  [ -d ${EXTRACT}/$dirname ] || tar xzf $spec -C ${EXTRACT} 
  [ -d ${EXTRACT}/$dirname ] && echo ${EXTRACT}/$dirname
}

NEW_DIR=$(resolve $NEW_VERSION)

if [ -z "${NEW_DIR}" ] || [ ! -d "${NEW_DIR}" ] ; then
    usage
fi

function banner() {
  echo ""
  echo "$@" | sed 's/./=/g'
  echo "$@"
  echo "$@" | sed 's/./=/g'
}

function listJarsInWar() {
  war=$1
  jar tvf $war | awk '/.jar$/{print "'"$war"'/"$8}'
}

function extractLicense() {
  war=$1
  rm -Rf tmpl
  mkdir tmpl
  cd tmpl
  jar xf ../$war META-INF/LICENSE
  cd ..
  cp tmpl/META-INF/LICENSE $2
  rm -Rf tmpl
}

function generateList() {
  dir=$1
  banner "Listing 3rd-party deps in ${dir##*/}"
  echo "**** ${dir##*/} jars ****" | tr '[:lower:]-' '[:upper:] ' > $dir/report1
  (cd $dir; find . -name '*.jar' | grep -v geode- | grep -v gfsh- | sort | sed 's#^./##' | tee -a report1)
  
  echo "**** ${dir##*/} wars ****" | tr '[:lower:]-' '[:upper:] ' > $dir/report2
  (cd $dir; find . -name '*.war' | sort | sed 's#^./##' | while read war ; do
    listJarsInWar $war | sed 's#-[v0-9][-0-9.SNAPSHOT]*[.]#.#' | sort
    extractLicense $war ${war%.war}.LICENSE
  done | tee -a report2)
}

generateList $NEW_DIR
if [ -n "${OLD_VERSION}" ] ; then
  OLD_DIR=$(resolve $OLD_VERSION)
  generateList $OLD_DIR

  banner "Diffing 3rd-party deps changes from ${OLD_DIR##*/} to ${NEW_DIR##*/}"
  for REPORT in report1 report2 ; do
    diff -y -W $(tput cols) $OLD_DIR/$REPORT $NEW_DIR/$REPORT | grep '[<|>]'
  done
fi

[ "$SKIP_LICENSES" = "true" ] && exit 0

function isApache2() {
  apache="HikariCP
accessors-smart
byte-buddy
classmate
commons-beanutils
commons-codec
commons-collections
commons-digester
commons-fileupload
commons-io
commons-lang3
commons-logging
commons-modeler
commons-text
commons-validator
content-type
error_prone_annotations
failureaccess
fastutil
findbugs-annotations
geo
guava
httpclient
httpcore
j2objc-annotations
jackson-
jcip-annotations
jna
json-path
json-smart
jsr305
jetty-
jgroups
jna-
lang-tag
listenablefuture
log4j-
lucene-
mapstruct
micrometer-core
netty-all
nimbus-jose-jwt
oauth2-oidc-sdk
rmiio
shiro-
snappy
spring-
springfox-
swagger-annotations
swagger-models"
  echo "$1" | egrep -q "(mx4j-remote|jaxb-api|$(echo -n "$apache" | tr '\n' '|'))"
}
function shortenDep() {
  echo "$1" | sed \
    -e 's/-api//' \
    -e 's/-impl//' \
    -e 's/-java//' \
    -e 's/shiro-.*/shiro-*/' \
    -e 's/jackson-.*/shiro-*/' \
    -e 's/jetty-.*/jetty-*/' \
    -e 's/jna-.*/jna-*/' \
    -e 's/lucene-.*/lucene-*/' \
    -e 's/log4j-.*/log4j-*/' \
    -e 's/mx4j-.*/mx4j*/' \
    -e 's/spring-.*/spring-*/' \
    -e 's/springfox-.*/springfox-*/'
}
for REPORT in report1 report2 ; do
  [ "$REPORT" = "report1" ] && topic=JAR || topic=WAR
  if [ "${licFromWs}" = "true" ] ; then
    [ "$REPORT" = "report1" ] && LICENSE=${root}/geode-assembly/src/main/dist/LICENSE || LICENSE=${root}/LICENSE
   else
    [ "$REPORT" = "report1" ] && LICENSE=${NEW_DIR}/LICENSE || LICENSE=${NEW_DIR}/tools/Pulse/$(cd ${NEW_DIR}/tools/Pulse; ls | grep LICENSE)
  fi
  LICENSE=${LICENSE#./}
  banner "Comparing $topic dep versions in ${NEW_DIR##*/} to $LICENSE"
  rm -f missing-$REPORT apache-$REPORT
  touch missing-$REPORT apache-$REPORT
  tail -n +2 $NEW_DIR/$REPORT | sed -e 's#.*/##' -e 's/.jar//' | sed 's/-\([0-9]\)/ \1/' | sort -u | grep -v '^ra$' | while read dep ver; do
    if isApache2 $dep ; then
      echo $dep $ver >> apache-$REPORT
    else
      echo $(shortenDep $dep) $ver
    fi
  done | sort -u | while read dep ver ; do
    if grep -qi "${dep/-/.}.*$ver" $LICENSE ; then
      echo "$dep $ver Found (and version matches)"
    elif grep -qi $dep $LICENSE ; then
      match="$(grep -i $dep $LICENSE | grep -v License | head -1)"
      if echo $match | grep -q '[0-9][0-9]*[.][0-9][0-9]*' ; then
        echo "$dep FOUND WITH A DIFFERENT VERSION, PLEASE UPDATE TO $ver:" >> missing-$REPORT
        echo "$match" >> missing-$REPORT
      else
        echo "$dep $ver probably found (without version):"
        echo "$match"
      fi
    else
      echo "$LICENSE FAILS TO MENTION $dep v$ver" >> missing-$REPORT
    fi
  done
  echo $(wc -l < apache-$REPORT) "deps are licensed under Apache 2.0 (no need to mention individually)"
  if [ $(wc -l < missing-$REPORT) -eq 0 ] ; then
    echo "All Good!"
  else
    cat missing-$REPORT
    result=1
  fi
done
exit $result
