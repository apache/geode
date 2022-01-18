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
      echo "Usage: print_announce_email.sh -v version_number -f latest_version_number"
      echo "  -v   The #.#.# version number"
      echo "  -f   The #.#.# version number of the latest and greatest, if other than above"
      exit 1
}

VERSION=""
LATER=""

while getopts ":v:f:" opt; do
  case ${opt} in
    v )
      VERSION=$OPTARG
      ;;
    f )
      LATER=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ ${VERSION} == "" ]] ; then
  usage
fi

if [[ $VERSION =~ ^([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    true
else
    echo "Malformed version number ${VERSION}. Example valid number - 1.9.0"
    exit 1
fi

if [ -z "$FLAGSHIP" ] || [[ "$FLAGSHIP" =~ ^([0-9]+\.[0-9]+\.[0-9]+)$ ]]; then
    true
else
    echo "Malformed version number ${FLAGSHIP}. Example valid number - 1.9.0"
    exit 1
fi

VERSION_MM=${VERSION%.*}

#support mac or linux date arithmetic syntax
DEADLINE=$(date --date '+5 days' '+%a, %B %d %Y' 2>/dev/null || date -v +5d "+%a, %B %d %Y" 2>/dev/null || echo "<5 days from now>")

if [ -n "${LATER}" ] && [ "${VERSION}" != "${LATER}" ] ; then
  LATEST="Users are encouraged to upgrade to the latest ${LATER%.*}.x release (currently $LATER)."
else
  LATEST="Users are encouraged to upgrade to this latest release."
fi

if echo $VERSION | grep -q '\.0$' ; then
  IMPROV=" improvements and"
else
  IMPROV=""
fi

cat << EOF
Subject: [ANNOUNCE] Apache Geode ${VERSION}
The Apache Geode community is pleased to announce the availability of
Apache Geode ${VERSION}.

Geode is a data management platform that provides a database-like consistency
model, reliable transaction processing and a shared-nothing architecture
to maintain very low latency performance with high concurrency processing.

Apache Geode ${VERSION} contains a number of${IMPROV} bug fixes.
$LATEST
For the full list of changes please review the release notes at:
https://cwiki.apache.org/confluence/display/GEODE/Release+Notes#ReleaseNotes-${VERSION}

Release artifacts and documentation can be found at the project website:
https://geode.apache.org/releases/
https://geode.apache.org/docs/guide/${VERSION_MM//./}/about_geode.html

We would like to thank all the contributors that made the release possible.
Regards,
$(git config --get user.name) on behalf of the Apache Geode team
EOF
