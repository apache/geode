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

FROM centos:centos7
MAINTAINER Geode Community <dev@geode.incubator.apache.org>

LABEL Vendor="Apache Geode"
LABEL version=unstable

# download JDK 8
ENV	JAVA_HOME /jdk1.8.0_51

RUN	yum install -y wget which tar git \
	&& wget --no-cookies --no-check-certificate --header "Cookie: gpw_e24=http%3A%2F%2Fwww.oracle.com%2F; oraclelicense=accept-securebackup-cookie" "http://download.oracle.com/otn-pub/java/jdk/8u51-b16/jdk-8u51-linux-x64.tar.gz" \
	&& tar xf jdk-8u51-linux-x64.tar.gz \
	&& rm -rf $JAVA_HOME/*src.zip \
						$JAVA_HOME/lib/missioncontrol \
						$JAVA_HOME/lib/visualvm \
						$JAVA_HOME/lib/*javafx* \
						$JAVA_HOME/jre/lib/plugin.jar \
						$JAVA_HOME/jre/lib/ext/jfxrt.jar \
						$JAVA_HOME/jre/bin/javaws \
						$JAVA_HOME/jre/lib/javaws.jar \
						$JAVA_HOME/jre/lib/desktop \
						$JAVA_HOME/jre/plugin \
						$JAVA_HOME/jre/lib/deploy* \
						$JAVA_HOME/jre/lib/*javafx* \
						$JAVA_HOME/jre/lib/*jfx* \
						$JAVA_HOME/jre/lib/amd64/libdecora_sse.so \
						$JAVA_HOME/jre/lib/amd64/libprism_*.so \
						$JAVA_HOME/jre/lib/amd64/libfxplugins.so \
						$JAVA_HOME/jre/lib/amd64/libglass.so \
						$JAVA_HOME/jre/lib/amd64/libgstreamer-lite.so \
						$JAVA_HOME/jre/lib/amd64/libjavafx*.so \
						$JAVA_HOME/jre/lib/amd64/libjfx*.so \
						jdk-8u51-linux-x64.tar.gz \
						/usr/share/locale/* \
	&& yum clean all

ENV PATH $PATH:$JAVA_HOME/bin

