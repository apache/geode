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

FROM geode/base:0.1
MAINTAINER Geode Community <dev@geode.incubator.apache.org>

LABEL Vendor="Apache Geode (incubating)"
LABEL version=unstable

# download JDK 8
# ENV	JAVA_HOME /jdk1.8.0_51

RUN	git clone -b develop https://github.com/apache/incubator-geode.git \
	&& cd incubator-geode \
	&& ./gradlew build -Dskip.tests=true \
	&& ls /incubator-geode | grep -v gemfire-assembly | xargs rm -rf \
	&& rm -rf /root/.gradle/ \
	&& rm -rf /incubator-geode/gemfire-assembly/build/distributions/ \
    && yum remove -y perl

ENV GEODE_HOME /incubator-geode/gemfire-assembly/build/install/apache-geode
ENV PATH $PATH:$GEODE_HOME/bin

# Default ports:
# RMI/JMX 1099
# REST 8080
# PULE 7070
# LOCATOR 10334
# CACHESERVER 40404
EXPOSE  8080 10334 40404 1099 7070
VOLUME ["/data/"]
CMD ["gfsh"]

