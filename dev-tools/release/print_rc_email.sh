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
      echo "Usage: print_rc_email.sh -v version_number -m maven_repo_id"
      echo "  -v   The #.#.#.RC# version number"
      echo "  -m   The 4 digit id of the nexus maven repo"
      exit 1
}

FULL_VERSION=""
MAVEN=""

while getopts ":v:m:" opt; do
  case ${opt} in
    v )
      FULL_VERSION=$OPTARG
      ;;
    m )
      MAVEN=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${FULL_VERSION} == "" ]] || [[ ${MAVEN} == "" ]]; then
  usage
fi

if [[ $FULL_VERSION =~ ^([0-9]+\.[0-9]+\.[0-9]+)\.(RC[0-9]+)$ ]]; then
    VERSION=${BASH_REMATCH[1]}
else
    echo "Malformed version number ${FULL_VERSION}. Example valid number - 1.9.0.RC1"
    exit 1
fi

VERSION_MM=${VERSION%.*}

#support mac or linux date arithmetic syntax
DEADLINE=$(date --date '+5 days' '+%a, %B %d %Y' 2>/dev/null || date -v +5d "+%a, %B %d %Y" 2>/dev/null || echo "<5 days from now>")

cat << EOF
Hello Geode Dev Community,

This is a release candidate for Apache Geode version ${FULL_VERSION}.
Thanks to all the community members for their contributions to this release!

Please do a review and give your feedback, including the checks you performed.

Voting deadline:
3PM PST ${DEADLINE}.

Please note that we are voting upon the source tag:
rel/v${FULL_VERSION}

Release notes:
https://cwiki.apache.org/confluence/display/GEODE/Release+Notes#ReleaseNotes-${VERSION}

Source and binary distributions:
https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/

Maven staging repo:
https://repository.apache.org/content/repositories/orgapachegeode-${MAVEN}

GitHub:
https://github.com/apache/geode/tree/rel/v${FULL_VERSION}
https://github.com/apache/geode-examples/tree/rel/v${FULL_VERSION}
https://github.com/apache/geode-native/tree/rel/v${FULL_VERSION}
https://github.com/apache/geode-benchmarks/tree/rel/v${FULL_VERSION}

Pipelines:
https://concourse.apachegeode-ci.info/teams/main/pipelines/apache-support-${VERSION_MM//./-}-main
https://concourse.apachegeode-ci.info/teams/main/pipelines/apache-support-${VERSION_MM//./-}-rc

Geode's KEYS file containing PGP keys we use to sign the release:
https://github.com/apache/geode/blob/develop/KEYS

Command to run geode-examples:
./gradlew -PgeodeReleaseUrl=https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION} -PgeodeRepositoryUrl=https://repository.apache.org/content/repositories/orgapachegeode-${MAVEN} build runAll

Regards
$(git config --get user.name)
EOF
