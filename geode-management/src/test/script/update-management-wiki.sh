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
if [[ ${BRANCH} != "develop" ]] && [[ ${BRANCH} != "master" ]] && [[ ${BRANCH%/*} != "support" ]] ; then
    echo "Please git checkout develop, master, or a support branch before running this script"
    exit 1
fi

SUDO=
if python --version | grep 'Python 3' ; then
  PYTHON=python
elif python3 --version | grep 'Python 3' ; then
  PYTHON=python3
else
  echo "please install python 3"
  exit 1
fi
if pip --version | grep 'python 3' ; then
  PIP=pip
elif pip3 --version | grep 'python 3' ; then
  PIP=pip3
else
  echo "please install pip 3"
  exit 1
fi

echo ""
echo "============================================================"
echo "Building"
echo "============================================================"
set -x
(cd "${0%/*}"/../../../.. && ./gradlew build installDist -x test -x javadoc -x rat -x pmdMain -x pmdTest)
set +x

GEODE=$(cd "${0%/*}"/../../../../geode-assembly/build/install/apache-geode; pwd)

if [ -x "$GEODE/bin/gfsh" ] ; then
    true
else
    echo "gfsh not found"
    exit 1
fi


GEODE_VERSION=$("$GEODE"/bin/gfsh version | sed -e 's/-SNAPSHOT//' -e 's/-build.*//')
[[ "${GEODE_VERSION%.*}" == "1.10" ]] && PAGE_ID=115511910
[[ "${GEODE_VERSION%.*}" == "1.11" ]] && PAGE_ID=135861023
[[ "${GEODE_VERSION%.*}" == "1.12" ]] && PAGE_ID=132322415
[[ "${GEODE_VERSION%.*}" == "1.13" ]] && PAGE_ID=147426059
[[ "${GEODE_VERSION%.*}" == "1.14" ]] && PAGE_ID=153817491
[[ "${GEODE_VERSION%.*}" == "1.15" ]] && PAGE_ID=188744355

if [[ -z "${PAGE_ID}" ]] ; then
    echo "Please create a new blank wiki page for $GEODE_VERSION under https://cwiki.apache.org/confluence/display/GEODE/Cluster+Management+Service+Rest+API and add its page ID to $0 near line 98"
    exit 1
fi


#skip generating steps if swagger output has already been generated
if ! [ -r static/index.html ] ; then

echo ""
echo "============================================================"
echo "Checking that swagger-codegen is installed (ignore warning/error if already installed)"
echo "============================================================"
brew install swagger-codegen || true
brew upgrade swagger-codegen || true

echo ""
echo "============================================================"
echo "Checking that premailer is installed (ignore warnings/errors if already installed)"
echo "============================================================"
$SUDO $PIP install premailer

echo ""
echo "============================================================"
echo "Starting up a locator to access swagger"
echo "============================================================"
set -x
ps -ef | grep swagger-locator | grep java | awk '{print $2}' | xargs kill -9
"$GEODE"/bin/gfsh "start locator --name=swagger-locator"
set +x

echo ""
echo "============================================================"
echo "Create static dir"
echo "============================================================"
set -x
if [[ ! -d "static" ]]
then
  mkdir "static"
fi
set +x

echo ""
echo "============================================================"
echo "Download swagger JSON"
echo "============================================================"
set -x
curl http://localhost:7070/management${URI_VERSION}/api-docs | jq . > static/swagger.json
set +x

echo ""
echo "============================================================"
echo "Generating docs"
echo "============================================================"
set -x
swagger-codegen generate -i static/swagger.json -l html -o static
set +x

echo ""
echo "============================================================"
echo "Stopping locator"
echo "============================================================"
set -x
ps -ef | grep swagger-locator | grep java | awk '{print $2}' | xargs kill -9
rm -Rf swagger-locator
set +x

fi #done skipping generating steps if was already generated

echo ""
echo "============================================================"
echo "Transforming docs"
echo "============================================================"
# we need to do a number of transforms to tweak the swagger output + make the content acceptable to confluence
cat static/index.html |
# clean up a few things premailer will otherwise choke on
grep -v doctype | sed -e 's/&mdash;/--/g' |
# convert css style block to inline css (that is the only way confluence accepts styling)
(set -o pipefail && $SUDO $PYTHON -m premailer --method xml --encoding ascii --pretty |
# strip off the document envelope (otherwise confluence will not accept it) by keeping only lines between the body tags
awk '
  /<\/body>/ {inbody=0}
  inbody==1  {print}
  /<body/   {inbody=1}
' |
# render linebreaks as <br>'s in preformatted blocks to avoid losing them later
awk '
  /<pre class="example".*><code/ {incode=1}
  /<\/code>/ {incode=0}
  incode==1  {print $0"<br/>"}
  incode!=1  {print}
' |
# insert cwiki TOC macro instead in place of unhelpful Methods/Access/Jump to Models/TOC
awk '
  BEGIN{print "<p><ac:structured-macro ac:name=\"toc\" ac:schema-version=\"1\" ac:macro-id=\"34f164af-2eb4-4135-8dd4-aad2ec630267\"><ac:parameter ac:name=\"maxLevel\">2</ac:parameter><ac:parameter ac:name=\"exclude\">Foo</ac:parameter></ac:structured-macro></p>"}
  />Access</ {skip=1}
  /<h1 style="font-size:25px"/{skip=0}
  skip!=1{print}
' |
# remove Models TOC and make Models section an h1
awk '
  /Jump to.*Methods/ {skip=1}
  /<div/{skip=0}
  /<h2.*Models/{gsub(/h2/,"h1")}
  skip!=1{print}
' |
#captialize http method names
sed -e 's#<span class="http-method" style="text-transform:uppercase">post</span>#POST#g' |
sed -e 's#<span class="http-method" style="text-transform:uppercase">get</span>#GET#g' |
sed -e 's#<span class="http-method" style="text-transform:uppercase">put</span>#PUT#g' |
sed -e 's#<span class="http-method" style="text-transform:uppercase">patch</span>#PATCH#g' |
sed -e 's#<span class="http-method" style="text-transform:uppercase">delete</span>#DELETE#g' |
# remove unhelpful lines about Produces */*
awk '
  />Produces</ {getline;getline;getline;getline;getline;getline;next}
  {print}
' |
# combine duplicate model name+desc
sed -e 's#pre;*">\([^<]*\)</code> - \1#pre">\1</code>#g' |
# prepend /management to /v1 and / links
sed -e "s#/management${URI_VERSION}#${URI_VERSION}#g" -e "s#${URI_VERSION}#/management${URI_VERSION}#g" -e "s#GET /<#GET /management<#g" |
# remove internal swagger endpoint id */*
sed -e 's/ .<span class="nickname" style="font-weight:bold">[^<]*<.span>.//' |
# remove Controller from endpoint categories
sed -e 's/DocLinksController/Versions/g' |
sed -e 's/PingManagementController/Ping/g' |
sed -e 's/ManagementController/ Management/g' |
sed -e 's/OperationController/ Operation/g' |
#don't link primitives
sed -e 's/<a href="#boolean">\([^<]*\)<.a>/\1/g' |
sed -e 's/<a href="#integer">\([^<]*\)<.a>/\1/g' |
sed -e 's/<a href="#long">\([^<]*\)<.a>/\1/g' |
sed -e 's/<a href="#double">\([^<]*\)<.a>/\1/g' |
sed -e 's/<a href="#string">\([^<]*\)<.a>/\1/g' |
sed -e 's/<a href="#object">\([^<]*\)<.a>/\1/g' |
#don't name the body parm
sed -e 's#Body Parameter</span> -- gatewayReceiverConfig#Request Body</span>#' |
# "links" : { } is unhelpful
sed -e 's#"links" : { }#"links" : { "self" : "uri" }#' |
# make endpoint banners into <h2>'s for TOC
awk '
  /div class="method-path"/{sub(/div/,"h2");print;getline;print;getline;sub(/\/div/,"/h2");print;next}
  {print}
' |
# remove "Up" links otherwise they bleed into TOC
sed -e 's#<a [^<]*float:right">Up</a>##' |
# remove fancy angle brackets
sed -e '/&#171;.*&#187;/s/,/_/g' |
sed -e 's/&#171;/_/g' -e 's/&#187;//g' |
#work around ever-increasing indent bug
sed -e 's/class="method" style="margin-left:20p/class="method" style="margin-left:0p/' |
# add more information at the top
awk '/More information:/{sub(/More information:/,"Swagger: http://locator:7070/management/docs (requires access to a running locator)<br/>Codegen: <code><small>brew install swagger-codegen; swagger-codegen generate -i http://locator:7070/management'${URI_VERSION}'/api-docs</small></code><br/>More information:")}{print}' |
cat > static/index-xhtml.html)
# if file is empty due to some error, abort!
! [ -z "$(cat static/index-xhtml.html)" ]

echo ""
echo "============================================================"
echo "Uploading to wiki"
echo "============================================================"
# get the current page version and ADD 1
PAGE_VERSION=$(curl -s -u $APACHE_CREDS https://cwiki.apache.org/confluence/rest/api/content/${PAGE_ID} | jq .version.number)
NEW_PAGE_VERSION=$(( PAGE_VERSION + 1 ))
# insert page content as the value of the "value" attribute in json update message
TITLE="${GEODE_VERSION} Management REST API - ${URI_VERSION#/}"
cat << EOF > static/body.json
{
  "id": "${PAGE_ID}",
  "type": "page",
  "title": "${TITLE}",
  "space": {
    "key": "GEODE"
  },
  "body": {
    "storage": {
      "value": "$(cat static/index-xhtml.html | sed 's/"/\\"/g')",
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
# upload
set -x
curl -u "$APACHE_CREDS" -X PUT -H 'Content-Type: application/json' -d @static/body.json https://cwiki.apache.org/confluence/rest/api/content/${PAGE_ID}
curl -v -S -u "$APACHE_CREDS" -X POST -H "X-Atlassian-Token: no-check" -F "file=@static/swagger.json" -F "comment=raw swagger json" "https://cwiki.apache.org/confluence/rest/api/content/${PAGE_ID}/child/attachment"
set +x

echo ""
echo "============================================================"
echo "Cleaning up"
echo "============================================================"
set -x
rm -Rf static
set +x

# open your web browser to the newly-updated page
open "https://cwiki.apache.org/confluence/display/pages/viewpage.action?pageId=${PAGE_ID}"