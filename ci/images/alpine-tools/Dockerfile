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

FROM alpine:3.14 as winrm-builder
RUN apk --no-cache add \
    git \
    go \
    musl-dev \
  && go get -v github.com/masterzen/winrm-cli

FROM google/cloud-sdk:alpine

COPY --from=winrm-builder /root/go/bin/winrm-cli /usr/local/bin/winrm
#COPY --from=hashicorp/packer:latest /bin/packer /usr/local/bin/packer
COPY --from=hashicorp/packer:1.4.5 /bin/packer /usr/local/bin/packer

RUN apk --no-cache add \
      bash \
      curl \
      git \
      jq \
      openssh-client \
      openssl \
      python3 \
      py3-crcmod \
      py3-openssl \
      py3-pip \
      py3-yaml \
      rsync \
      util-linux \
      wget \
  && echo "https://apk.bell-sw.com/main" | tee -a /etc/apk/repositories \
  && wget -P /etc/apk/keys/ https://apk.bell-sw.com/info@bell-sw.com-5fea454e.rsa.pub \
  && apk add --no-cache \
    bellsoft-java8 \
  && apk del \
      wget \
  && gcloud config set core/disable_usage_reporting true \
  && gcloud config set component_manager/disable_update_check true \
  && gcloud config set metrics/environment github_docker_image \
  && gcloud components install -q beta \
  && printf "Host *\n  ServerAliveInterval 60 \n  ServerAliveCountMax 2\n" >> /etc/ssh/ssh_config \
  && pip3 install awscli

ENV JAVA_HOME /usr/lib/jvm/default-jvm
ENV PATH="$PATH:$JAVA_HOME/bin"
