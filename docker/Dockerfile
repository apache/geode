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

FROM ubuntu
LABEL maintainer Apache Geode <dev@geode.apache.org>

ARG GEODE_VERSION

RUN apt-get update
RUN apt-get install -y build-essential cmake doxygen git openjdk-8-jdk \
                       wget zlib1g-dev

RUN \
  wget "https://www.apache.org/dyn/closer.cgi?action=download&filename=geode/1.0.0-incubating/apache-geode-${GEODE_VERSION}.tar.gz" && \
  tar xzf "closer.cgi?action=download&filename=geode%2F1.0.0-incubating%2Fapache-geode-${GEODE_VERSION}.tar.gz" && \
  rm "closer.cgi?action=download&filename=geode%2F1.0.0-incubating%2Fapache-geode-${GEODE_VERSION}.tar.gz"

ENV GEODE /apache-geode-${GEODE_VERSION}
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

CMD ["bash"]
