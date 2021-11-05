# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM bellsoft/liberica-openjdk-alpine:11
#FROM bellsoft/liberica-openjdk-alpine:8

# runtime dependencies
RUN apk add --no-cache \
		bash \
		net-tools \
		ncurses

ENV GEODE_HOME /geode
ENV PATH $PATH:$GEODE_HOME/bin

RUN set -eux; \
	apk add --no-cache --virtual .fetch-deps \
		libressl \
		gnupg \
	;
COPY geode /geode
COPY "geode/launch.sh" .
RUN chmod +x launch.sh
EXPOSE  9090 9091 10334 40404 1099 7070 6379 5678
HEALTHCHECK CMD ls -l / | grep "launched.txt" > /dev/null; if [ 0 != $? ]; then exit 1; fi;
