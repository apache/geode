#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


ATTEMPTS=5
FAILED=0
# Fetch public key using HTTP
while [ ! -f ~/.ssh/authorized_keys ]; do
    tmp_keys=`mktemp -p . -t authorized_keys.XXXXX` || exit $?
    curl -f http://169.254.169.254/latest/meta-data/public-keys/0/openssh-key > $tmp_keys 2>/dev/null
    if [ $? -eq 0 ]; then
        grep -f $tmp_keys ~/.ssh/authorized_keys 2>/dev/null
        if [ $? -eq 0 ]; then
            echo "AWS public key was already in authorized_keys, not adding it again"
        else
            if [ ! -d ~/.ssh ]; then
                mkdir ~/.ssh
				chmod 700 ~/.ssh
				restorecon ~/.ssh
            fi
            cat $tmp_keys >> ~/.ssh/authorized_keys
            chmod 0600 ~/.ssh/authorized_keys
            restorecon ~/.ssh/authorized_keys
            echo "Successfully retrieved AWS public key from instance metadata"
        fi
    else
        FAILED=$(($FAILED + 1))
        if [ $FAILED -ge $ATTEMPTS ]; then
            echo "Failed to retrieve AWS public key after $FAILED attempts, quitting"
            rm -f
            break
        fi
        echo "Could not retrieve AWS public key (attempt #$FAILED/$ATTEMPTS), retrying in 5 seconds..."
        sleep 5
    fi
    rm -f $tmp_keys
done
