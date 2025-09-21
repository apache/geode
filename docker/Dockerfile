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

FROM bellsoft/liberica-openjdk-alpine:8


# runtime dependencies
RUN apk add --no-cache \
		bash \
		ncurses

# https://github.com/apache/geode/blob/develop/KEYS
ENV GEODE_GPG 62F7DA41B7D8F26C

ENV GEODE_HOME /geode
ENV PATH $PATH:$GEODE_HOME/bin

# https://geode.apache.org/releases/
ENV GEODE_VERSION 1.15.2
# Binaries TGZ SHA-256
# https://dist.apache.org/repos/dist/release/geode/VERSION/apache-geode-VERSION.tgz.sha256
ENV GEODE_SHA256 60d190b07b4dabd83a86bfa21acab7ed38d2eccaabe4bc5baabab0981cf7910a

# http://apache.org/dyn/closer.cgi/geode/1.3.0/apache-geode-1.3.0.tgz

RUN set -eux; \
	apk add --no-cache --virtual .fetch-deps \
		libressl \
		gnupg \
	; \
	for file in \
		"geode/$GEODE_VERSION/apache-geode-$GEODE_VERSION.tgz" \
		"geode/$GEODE_VERSION/apache-geode-$GEODE_VERSION.tgz.asc" \
	; do \
		target="$(basename "$file")"; \
		for url in \
			"https://www.apache.org/dyn/closer.lua/$file?action=download" \
			"https://downloads.apache.org/$file" \
			"https://archive.apache.org/dist/$file" \
		; do \
			if wget -O "$target" "$url"; then \
				break; \
			fi; \
		done; \
	done; \
	[ -s "apache-geode-$GEODE_VERSION.tgz" ]; \
	[ -s "apache-geode-$GEODE_VERSION.tgz.asc" ]; \
	echo "$GEODE_SHA256 *apache-geode-$GEODE_VERSION.tgz" | sha256sum -c -; \
	export GNUPGHOME="$(mktemp -d)"; \
	gpg --keyserver keyserver.ubuntu.com --recv-keys "$GEODE_GPG"; \
	gpg --batch --verify "apache-geode-$GEODE_VERSION.tgz.asc" "apache-geode-$GEODE_VERSION.tgz"; \
	rm -rf "$GNUPGHOME"; \
	mkdir /geode; \
	tar --extract \
		--file "apache-geode-$GEODE_VERSION.tgz" \
		--directory /geode \
		--strip-components 1 \
	; \
	rm -rf /geode/javadoc "apache-geode-$GEODE_VERSION.tgz" "apache-geode-$GEODE_VERSION.tgz.asc"; \
	apk del .fetch-deps; \
# smoke test to ensure the shell can still run properly after removing temporary deps
	gfsh version

# Default ports:
# RMI/JMX 1099
# REST 8080
# PULE 7070
# LOCATOR 10334
# CACHESERVER 40404
EXPOSE  8080 10334 40404 1099 7070
VOLUME ["/data"]
CMD ["gfsh"]
