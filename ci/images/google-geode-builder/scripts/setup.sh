#!/usr/bin/env bash
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

set -x
set -e
export CLOUD_SDK_VERSION=209.0.0
export CHROME_DRIVER_VERSION=2.35
export LOCAL_USER=geode
export LOCAL_UID=93043

apt-get update
apt-get install -y --no-install-recommends \
  apt-transport-https \
  lsb-release

echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list
echo "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
curl -sSL https://dl.google.com/linux/linux_signing_key.pub | apt-key add -
curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
apt-get update
apt-get purge -y google-cloud-sdk lxc-docker
apt-get install -y --no-install-recommends \
    aptitude \
    ca-certificates \
    cgroupfs-mount \
    docker-compose \
    docker-ce \
    git \
    google-chrome-stable \
    htop \
    jq \
    less \
    lsof \
    net-tools \
    python3 \
    python3-pip \
    rsync \
    tmux \
    unzip \
    vim

cp -R /etc/alternatives /etc/keep-alternatives
apt-get install -y --no-install-recommends \
    openjdk-8-jdk
rm -rf /etc/alternatives
mv /etc/keep-alternatives /etc/alternatives

JDK_URL=$(curl -Ls http://jdk.java.net/11 | awk '/linux-x64/{sub(/.*href=./,"");sub(/".*/,"");if(found!=1)print;found=1}')
tar xzf <(curl -s $JDK_URL) -C /usr/lib/jvm
mv /usr/lib/jvm/jdk-11* /usr/lib/jvm/java-11-openjdk-amd64

pushd /tmp
  curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz
  tar xzf google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz -C /
  rm google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz
  curl -sSO https://dl.google.com/cloudagents/install-monitoring-agent.sh
  bash install-monitoring-agent.sh
  rm install-monitoring-agent.sh
popd
export PATH=/google-cloud-sdk/bin:${PATH}
gcloud config set core/disable_usage_reporting true
gcloud config set component_manager/disable_update_check true
gcloud config set metrics/environment github_docker_image
gcloud components install docker-credential-gcr --quiet
gcloud auth configure-docker --quiet
docker pull ${GEODE_DOCKER_IMAGE}
curl -Lo /usr/local/bin/dunit-progress https://github.com/jdeppe-pivotal/progress-util/releases/download/0.2/progress.linux
chmod +x /usr/local/bin/dunit-progress
wget --no-verbose -O /tmp/chromedriver_linux64.zip https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip
rm -rf /opt/selenium/chromedriver
unzip /tmp/chromedriver_linux64.zip -d /opt/selenium
rm /tmp/chromedriver_linux64.zip
mv /opt/selenium/chromedriver /opt/selenium/chromedriver-${CHROME_DRIVER_VERSION}
chmod 755 /opt/selenium/chromedriver-${CHROME_DRIVER_VERSION}
ln -fs /opt/selenium/chromedriver-${CHROME_DRIVER_VERSION} /usr/bin/chromedriver
adduser --disabled-password --gecos "" --uid ${LOCAL_UID} ${LOCAL_USER}
usermod -G docker,google-sudoers -a ${LOCAL_USER}
echo "export PATH=/google-cloud-sdk/bin:${PATH}" > /etc/profile.d/google_sdk_path.sh

apt-get clean
rm -rf /var/lib/apt/lists/*
