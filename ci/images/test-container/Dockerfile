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

FROM ubuntu:bionic
ENTRYPOINT []

ARG CHROME_DRIVER_VERSION=2.35

WORKDIR /tmp/work
RUN  apt-get update \
  && apt-get install -y --no-install-recommends \
     apt-transport-https \
     ca-certificates \
     bzip2 \
     curl \
     git \
     gnupg \
     lsb-release \
     lsof \
     net-tools \
     procps \
     software-properties-common \
     unzip \
     xz-utils \
  && echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
  && echo "deb [arch=amd64] https://apt.bell-sw.com/ stable main" > /etc/apt/sources.list.d/bellsoft.list \
  && curl -sSL https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
  && curl -fsSL https://download.bell-sw.com/pki/GPG-KEY-bellsoft | apt-key add - \
  && apt-get update \
  && apt-get install -y --no-install-recommends \
    aptitude \
    google-chrome-stable \
  && wget --no-verbose -O /tmp/chromedriver_linux64.zip https://chromedriver.storage.googleapis.com/$CHROME_DRIVER_VERSION/chromedriver_linux64.zip \
  && rm -rf /opt/selenium/chromedriver \
  && unzip /tmp/chromedriver_linux64.zip -d /opt/selenium \
  && rm /tmp/chromedriver_linux64.zip \
  && mv /opt/selenium/chromedriver /opt/selenium/chromedriver-$CHROME_DRIVER_VERSION \
  && chmod 755 /opt/selenium/chromedriver-$CHROME_DRIVER_VERSION \
  && ln -fs /opt/selenium/chromedriver-$CHROME_DRIVER_VERSION /usr/bin/chromedriver \
  && useradd --shell /bin/bash -u 93043 -o -c "" -m geode \
  && cp -R /etc/alternatives /etc/keep-alternatives \
  && apt-get install -y --no-install-recommends \
    java-common \
    bellsoft-java11 \
    bellsoft-java8 \
  && update-java-alternatives -s bellsoft-java8-amd64 \
  && rm -rf /etc/alternatives \
  && mv /etc/keep-alternatives /etc/alternatives \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && rm -rf /tmp/work
