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
FROM openjdk:8
ENTRYPOINT []

# apt prerequisites for installing docker and gcloud
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
RUN apt-key adv --keyserver hkp://pgp.mit.edu --recv-keys 58118E89F3A912897C070ADBF76221572C52609D
RUN apt-get update
RUN apt-get install -y apt-transport-https ca-certificates aptitude lsb-release jq unzip vim htop golang cgroupfs-mount python3 python3-pip
ADD docker.list /etc/apt/sources.list.d/
RUN echo "deb http://packages.cloud.google.com/apt cloud-sdk-$(lsb_release -c -s) main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# Install docker and gcloud
RUN apt-get update
RUN apt-get purge lxc-docker
RUN apt-get install -y docker-engine google-cloud-sdk && apt-get clean

# For a CI tool, disable updates to gcloud since they'll be thrown away at end of run
RUN gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true && \
    gcloud config set metrics/environment github_docker_image

# Install Tini and associated wrapper
ADD https://github.com/krallin/tini/releases/download/v0.14.0/tini-static-amd64 /usr/local/bin/tini
RUN chmod +x /usr/local/bin/tini
ADD tini-wrapper.go .
RUN go build -o /usr/local/bin/tini-wrapper ./tini-wrapper.go

# Init script for docker inside our workers
ADD ./initdocker /usr/local/bin/initdocker
RUN chmod +x /usr/local/bin/initdocker

# Install dunit-process
RUN curl -Lo /usr/local/bin/dunit-progress https://github.com/jdeppe-pivotal/progress-util/releases/download/0.2/progress.linux
RUN chmod +x /usr/local/bin/dunit-progress

# Install Chrome
RUN curl -sSL https://dl.google.com/linux/linux_signing_key.pub | apt-key add - \
	&& echo "deb [arch=amd64] https://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list \
	&& apt-get update && apt-get install -y google-chrome-stable --no-install-recommends \
	&& rm -rf /var/lib/apt/lists/*

# Install Chrome driver
ARG CHROME_DRIVER_VERSION=2.35
RUN wget --no-verbose -O /tmp/chromedriver_linux64.zip https://chromedriver.storage.googleapis.com/$CHROME_DRIVER_VERSION/chromedriver_linux64.zip \
  && rm -rf /opt/selenium/chromedriver \
  && unzip /tmp/chromedriver_linux64.zip -d /opt/selenium \
  && rm /tmp/chromedriver_linux64.zip \
  && mv /opt/selenium/chromedriver /opt/selenium/chromedriver-$CHROME_DRIVER_VERSION \
  && chmod 755 /opt/selenium/chromedriver-$CHROME_DRIVER_VERSION \
  && ln -fs /opt/selenium/chromedriver-$CHROME_DRIVER_VERSION /usr/bin/chromedriver

# Cache gradle for performance
ENV GRADLE_USER_HOME /usr/local/maven_files
WORKDIR /tmp/work
ADD cache_dependencies.sh cache_dependencies.sh
RUN chmod +x cache_dependencies.sh
RUN ./cache_dependencies.sh && rm -rf /tmp/work
