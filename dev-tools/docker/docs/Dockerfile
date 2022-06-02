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

FROM ruby:2.6.8

LABEL Vendor="Apache Geode"
LABEL version=unstable
LABEL maintainer=dev@geode.apache.org

# Nodejs & gems needed for 'rackup'
RUN curl -fsSL https://deb.nodesource.com/setup_16.x | bash - ; \
    apt-get install -y nodejs
RUN gem install bundler:1.17.3 \
    rake multi_json:1.13.1 \
    elasticsearch:7.5.0 \
    multipart-post:2.0.0 \
    faraday:0.17.4 \
    libv8:3.16.14.15 \
    mini_portile2:2.8.0 \
    racc:1.6.0 \
    nokogiri:1.13.3 \
    mimemagic:0.3.10 \
    puma:5.6.2 \
    rack:2.2.3 \
    smtpapi:0.1.0 \
    sendgrid-ruby:1.1.6 \
    therubyracer:0.12.2

# Install Bookbinder
COPY Gemfile Gemfile
COPY Gemfile.lock Gemfile.lock

RUN bundle install
