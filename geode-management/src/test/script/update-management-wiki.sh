#! /usr/bin/env bash
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

URI_VERSION=/v1

set -e

usage() {
    echo "Usage: update-management-wiki.sh -u apache_username:apache_password"
    echo '  -u   Your id.apache.org creds in the form user:pass.  You can also set $APACHE_CREDS'
    exit 1
}

while getopts ":u:" opt; do
  case ${opt} in
    u )
      APACHE_CREDS=$OPTARG
      ;;
    \? )
      usage
      ;;
  esac
done

if [[ "${APACHE_CREDS}" == "" ]] ; then
    usage
fi

if [[ "${APACHE_CREDS}" =~ : ]]; then
    true
else
    echo "Please specify creds in the form user:pass"
    exit 1
fi

BRANCH=$(git rev-parse --abbrev-ref HEAD)
if [[ ${BRANCH} != "develop" ]] && [[ ${BRANCH} != "master" ]] ; then
    echo "Please git checkout develop or git checkout master before running this script"
    exit 1
fi

MASTER_PAGE_ID=115511910
DEVELOP_PAGE_ID=132322415
[[ "${BRANCH}" == "master" ]] && PAGE_ID=$MASTER_PAGE_ID || PAGE_ID=$DEVELOP_PAGE_ID

echo ""
echo "============================================================"
echo "Building"
echo "============================================================"
set -x
(cd ${0%/*}/../../../.. && ./gradlew build installDist -x test -x javadoc -x rat -x pmdMain -x pmdTest)
set +x

GEODE=$(cd ${0%/*}/../../../../geode-assembly/build/install/apache-geode; pwd)

if [ -x "$GEODE/bin/gfsh" ] ; then
    true
else
    echo "gfsh not found"
    exit 1
fi

echo ""
echo "============================================================"
echo "Checking that swagger-codegen is installed"
echo "============================================================"
brew install swagger-codegen || true
brew upgrade swagger-codegen || true

echo ""
echo "============================================================"
echo "Starting up a locator to access swagger"
echo "============================================================"
set -x
ps -ef | grep swagger-locator | grep java | awk '{print $2}' | xargs kill -9
[[ "${BRANCH}" == "master" ]] && GEODE_VERSION=$($GEODE/bin/gfsh version) || GEODE_VERSION=develop
$GEODE/bin/gfsh "start locator --name=swagger-locator"
set +x

echo ""
echo "============================================================"
echo "Generating docs"
echo "============================================================"
set -x
swagger-codegen generate -i http://localhost:7070/management${URI_VERSION}/api-docs -l html -o static
set +x

echo ""
echo "============================================================"
echo "Uploading to wiki"
echo "============================================================"
# step 1a: strip out body/header/html envelope
# step 1b: add line breaks in code examples, since otherwise they inexplicably get lost in the upload
# step 1c: escape quotes and newlines
# step 1d: future: do any search-and-replaces to make it prettier here, such as to make up for lost css
VALUE=$(cat static/index.html | awk '
  /<\/body>/ {inbody=0}
  inbody==1  {print}
  /<body>/   {inbody=1}
' | awk '
  /<pre class="example"><code>/ {incode=1}
  incode==1  {print $0"<br/>"}
  incode!=1  {print}
  /<\/code>/ {incode=0}
' | sed 's/"/\\"/g')
# step 2: get the current page version and ADD 1
PAGE_VERSION=$(curl -s -u $APACHE_CREDS https://cwiki.apache.org/confluence/rest/api/content/${PAGE_ID} | jq .version.number)
NEW_PAGE_VERSION=$(( PAGE_VERSION + 1 ))
# step 3: insert as value into json update message
TITLE="${GEODE_VERSION} Management REST API - ${URI_VERSION#/}"
cat << EOF > body.json
{
  "id": "${PAGE_ID}",
  "type": "page",
  "title": "${TITLE}",
  "space": {
    "key": "GEODE"
  },
  "body": {
    "storage": {
      "value": "${VALUE}",
      "representation": "storage"
    }
  },
  "version": {
    "number": ${NEW_PAGE_VERSION},
    "minorEdit": true,
    "message": "automatically generated from ${GEODE_VERSION} by update-management-wiki.sh"
  }
}
EOF
#step 4: upload to the wiki
set -x
curl -u $APACHE_CREDS -X PUT -H 'Content-Type: application/json' -d @body.json https://cwiki.apache.org/confluence/rest/api/content/${PAGE_ID}
set +x

echo ""
echo "============================================================"
echo "Cleaning up"
echo "============================================================"
set -x
ps -ef | grep swagger-locator | grep java | awk '{print $2}' | xargs kill -9
rm -Rf swagger-locator
rm -Rf static
rm body.json
set +x

# open your web browser to the newly-updated page
open "https://cwiki.apache.org/confluence/display/pages/viewpage.action?pageId=${PAGE_ID}"