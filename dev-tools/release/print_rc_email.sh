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

#support mac or linux date arithmetic syntax
DEADLINE=$(date --date '+5 days' '+%a, %B %d %Y' 2>/dev/null || date -v +5d "+%a, %B %d %Y" 2>/dev/null || echo "<5 days from now>")

cat << EOF
Hello Geode dev community,

This is a release candidate for Apache Geode, version ${FULL_VERSION}.
Thanks to all the community members for their contributions to this release!

Please do a review and give your feedback. The deadline is 3PM PST ${DEADLINE}.
Release notes can be found at: https://cwiki.apache.org/confluence/display/GEODE/Release+Notes#ReleaseNotes-${VERSION}

Please note that we are voting upon the source tags: rel/v${FULL_VERSION}

Apache Geode:
https://github.com/apache/geode/tree/rel/v${FULL_VERSION}
Apache Geode examples:
https://github.com/apache/geode-examples/tree/rel/v${FULL_VERSION}
Apache Geode native:
https://github.com/apache/geode-native/tree/rel/v${FULL_VERSION}

Source and binary files:
https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION}/

Maven staging repo:
https://repository.apache.org/content/repositories/orgapachegeode-${MAVEN}

Geode's KEYS file containing PGP keys we use to sign the release:
https://github.com/apache/geode/blob/develop/KEYS

PS: Command to run geode-examples: ./gradlew -PgeodeReleaseUrl=https://dist.apache.org/repos/dist/dev/geode/${FULL_VERSION} -PgeodeRepositoryUrl=https://repository.apache.org/content/repositories/orgapachegeode-${MAVEN} build runAll

Regards
$(git config --get user.name)
EOF
