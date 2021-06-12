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
    echo "  -s   No source license check (just check the binary license)"
    exit 1
}


while getopts ":v:p:ns" opt; do
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
    s )
      SKIP_SRC_LICENSE=true
      ;;
    \? )
      usage
      ;;
  esac
done

if [ -z "${NEW_VERSION}" ] ; then
    usage
fi

WORKSPACE=$(pwd)/license_tmp
DOWNLOAD=${WORKSPACE}/download
EXTRACT=${WORKSPACE}/extracted
mkdir -p ${DOWNLOAD}
mkdir -p ${EXTRACT}
root=$0
root=${root%/dev-tools*}

if [ "$NEW_VERSION" = "HEAD" ] ; then
  licFromWs=true
  rm -Rf $root/geode-assembly/build/distributions
fi


function resolve() {
  [ -n "$1" ] || return
  spec=$1
  suffix=$2
  if [ "HEAD" = "$spec" ] ; then
    [ "${suffix}" = "-src" ] && target=srcDistTar || target=distTar
    (cd $root && ./gradlew ${target} 1>&2)
    spec=$root/geode-assembly/build/distributions/$(cd $root/geode-assembly/build/distributions && ls -t | grep -v sha256 | grep "apache-geode-.*-build.[0-9][0-9]*${suffix}.tgz" | tail -1)
    [ -r "$spec" ] || echo "Build not found: $spec" 1>&2
    [ -r "$spec" ]
  fi

  if [[ $spec =~ ^([0-9]+\.[0-9]+\.[0-9]+)\.(RC[0-9]+)$ ]]; then
    mmp=$(echo $spec | sed 's/.RC.*//')
    #bare RC version -> RC url
    spec=https://dist.apache.org/repos/dist/dev/geode/${spec}/apache-geode-${mmp}${suffix}.tgz
  elif [[ $spec =~ ^([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    #bare released version -> release url
    spec=https://downloads.apache.org/geode/${spec}/apache-geode-${spec}${suffix}.tgz
  elif echo "$spec" | grep -q '^http.*tgz$' ; then
    #tgz url
    echo "$spec" | grep -q -- "${suffix}.tgz$" || return
  elif [ -r "$spec" ] && echo "$spec" | grep -q 'tgz$' ; then
    #tgz file present locally
    echo "$spec" | grep -q -- "${suffix}.tgz$" || return
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

if [ "${licFromWs}" = "true" ] && ! [ "$SKIP_LICENSES" = "true" ] && ! [ "$SKIP_SRC_LICENSE" = "true" ] ; then
  NEW_SRC_DIR=$(resolve $NEW_VERSION -src)
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

  #also extract geode jar licenses for later checking
  (cd $dir; find . -name '*.jar' | egrep '(geode|gfsh)-' | sort | sed 's#^./##' | while read geodejar ; do
    extractLicense $geodejar ${geodejar%.jar}.LICENSE
  done)

  echo "**** ${dir##*/} jars ****" | tr '[:lower:]-' '[:upper:] ' > $dir/report1
  (cd $dir; find . -name '*.jar' | grep -v geode- | grep -v gfsh- | sort | sed 's#^./##' | tee -a report1)

  echo "**** ${dir##*/} wars ****" | tr '[:lower:]-' '[:upper:] ' > $dir/report2
  (cd $dir; find . -name '*.war' | sort | sed 's#^./##' | while read war ; do
    listJarsInWar $war | sed 's#-[v0-9][-0-9.SNAPSHOTbuild]*[.]#.#' | sort
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

banner "Checking that all binary licenses are identical"
sizes=$(find $NEW_DIR -name '*LICENSE' | xargs wc -c | grep -v total | awk '{print $1}' | sort -u | wc -l)
if [ $sizes -gt 1 ] ; then
  echo "NOT all LICENSES are the same:"
  (cd $NEW_DIR; find * -name '*LICENSE' | xargs wc -c | grep -v total | sort)
  result=1
else
  echo 'All Good!'
fi

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
commons-math3
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
grumpy-
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
    LICENSE=${root}/geode-assembly/src/main/dist/LICENSE
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
    if grep -qi "${dep//-/.}.*$ver" $LICENSE ; then
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
  rm apache-$REPORT
  if [ $(wc -l < missing-$REPORT) -eq 0 ] ; then
    echo 'All Good!'
  else
    cat missing-$REPORT
    rm missing-$REPORT
    result=1
  fi
done

function checkMissing() {
  rm -f missing
  touch missing
  grep '^  - ' | sed -e 's/^  - //' -e 's/, .*//' -e 's/ (.*//' -e 's/s* v.*//' -e 's/ /.?/g' | while read f; do 
    if (cd ${root} && git grep -Eqi "$f" -- ':!LICENSE' ':!**/LICENSE' ':!NOTICE' ':!**/NOTICE') ; then
      true
      #echo "${f//\?/} found"
    else
      echo "${f//\?/} appears to be unused.  Please remove from $1" >> missing
    fi
  done
  if [ $(wc -l < missing) -eq 0 ] ; then
    echo 'All Good!'
    rm missing
  else
    cat missing
    rm missing
    return 1
  fi
}

if [ "${licFromWs}" = "true" ] ; then
  banner "Checking that binary license is a superset of src license"
  SLICENSE=${root}/LICENSE
  BLICENSE=${root}/geode-assembly/src/main/dist/LICENSE
  if diff $SLICENSE $BLICENSE | grep -q '^<' ; then
    echo $(diff $SLICENSE $BLICENSE | grep '^<' | wc -l) "lines appear in $SLICENSE that were not found in $BLICENSE."
    echo "Please ensure the binary license is a strict superset of the source license."
    echo "(diff $SLICENSE $BLICENSE)"
    result=1
  else
    echo 'All Good!'
  fi

  banner "Checking that binary license is correct"
  if diff -q ${BLICENSE} ${NEW_DIR}/LICENSE ; then
    echo 'All Good!'
  else
    echo "Incorrect LICENSE in binary distribution"
    echo "Expected:" $(wc -c ${BLICENSE})
    echo "Actual:" $(wc -c ${NEW_DIR}/LICENSE)
  fi

  if ! [ "$SKIP_SRC_LICENSE" = "true" ] ; then
    banner "Checking that source license is correct"
    if diff -q ${SLICENSE} ${NEW_SRC_DIR}/LICENSE ; then
      echo 'All Good!'
    else
      echo "Incorrect LICENSE in source distribution"
      echo "Expected:" $(wc -c ${SLICENSE})
      echo "Actual:" $(wc -c ${NEW_SRC_DIR}/LICENSE)
    fi

    banner "Checking references in source license"
    cat $SLICENSE | checkMissing $SLICENSE

    banner "Checking references in binary license"
    cat $SLICENSE $SLICENSE $BLICENSE | sort | uniq -u | checkMissing $BLICENSE
  fi
fi

exit $result
