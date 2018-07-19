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

export CHROME_DRIVER_VERSION=2.35
export GRADLE_USER_HOME=/usr/local/maven_files
export LOCAL_USER=geode

chmod +x /usr/local/bin/initdocker
mkdir -p ${GRADLE_USER_HOME}
apt-get update
apt-get install -y --no-install-recommends \
  apt-transport-https \
  lsb-release

echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list
echo "deb http://packages.cloud.google.com/apt cloud-sdk-$(lsb_release -c -s) main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
echo "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
curl -sSL https://dl.google.com/linux/linux_signing_key.pub | apt-key add -
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key add -
curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
apt-get update
apt-get purge lxc-docker
apt-get install -y --no-install-recommends \
    aptitude \
    ca-certificates \
    cgroupfs-mount \
    docker-compose \
    docker-ce \
    git \
    golang \
    google-chrome-stable \
    google-cloud-sdk \
    htop \
    jq \
    openjdk-8-jdk-headless \
    python3 \
    python3-pip \
    rsync \
    unzip \
    vim
gcloud config set core/disable_usage_reporting true
gcloud config set component_manager/disable_update_check true
gcloud config set metrics/environment github_docker_image
curl -Lo /usr/local/bin/dunit-progress https://github.com/jdeppe-pivotal/progress-util/releases/download/0.2/progress.linux
chmod +x /usr/local/bin/dunit-progress
wget --no-verbose -O /tmp/chromedriver_linux64.zip https://chromedriver.storage.googleapis.com/${CHROME_DRIVER_VERSION}/chromedriver_linux64.zip
rm -rf /opt/selenium/chromedriver
unzip /tmp/chromedriver_linux64.zip -d /opt/selenium
rm /tmp/chromedriver_linux64.zip
mv /opt/selenium/chromedriver /opt/selenium/chromedriver-${CHROME_DRIVER_VERSION}
chmod 755 /opt/selenium/chromedriver-${CHROME_DRIVER_VERSION}
ln -fs /opt/selenium/chromedriver-${CHROME_DRIVER_VERSION} /usr/bin/chromedriver
adduser --disabled-password --gecos "" ${LOCAL_USER}
usermod -G docker,google-sudoers -a ${LOCAL_USER}
chown -R ${LOCAL_USER} ${GRADLE_USER_HOME}
echo "export GRADLE_USER_HOME=${GRADLE_USER_HOME}" > /etc/profile.d/gradle_home.sh
apt-get clean
rm -rf /var/lib/apt/lists/*
