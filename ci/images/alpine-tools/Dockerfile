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
FROM openjdk:8-jdk-alpine

COPY --from=google/cloud-sdk:alpine /google-cloud-sdk /google-cloud-sdk
COPY --from=hashicorp/packer:latest /bin/packer /usr/local/bin/packer
ENV PATH /google-cloud-sdk/bin:$PATH
RUN apk --no-cache add \
      bash \
      curl \
      git \
      go \
      jq \
      make \
      musl-dev \
      openssh-client \
      openssl \
      python \
      py2-pip \
      rsync \
      util-linux \
  && gcloud config set core/disable_usage_reporting true \
  && gcloud config set component_manager/disable_update_check true \
  && gcloud config set metrics/environment github_docker_image \
  && git clone https://github.com/masterzen/winrm-cli \
  && (cd winrm-cli; GOPATH=$PWD PATH=$PATH:$PWD/bin make) \
  && cp winrm-cli/bin/winrm /usr/local/bin/ \
  && rm -rf winrm-cli \
  && gcloud components install -q beta \
  && printf "Host *\n  ServerAliveInterval 60 \n  ServerAliveCountMax 2\n" >> /etc/ssh/ssh_config \
  && pip2 install awscli \
  && apk --no-cache del go make
