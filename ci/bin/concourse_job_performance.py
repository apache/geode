#!/usr/bin/env python3
#
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
#

import os
import requests
import json
import sseclient
import time
from colors import *
from urllib.parse import urlparse
from operator import itemgetter

if len(sys.argv) != 5:
    print("Usage: {} <concourse url> <pipeline> <job> <count>".format(os.path.basename(sys.argv[0])))
    exit(1)

url = sys.argv[1]
pipeline = sys.argv[2]
job = sys.argv[3]
build_count = int(sys.argv[4])
concourse_url = urlparse(url)

completed_build_states = ['succeeded', 'failed']
if not concourse_url.scheme or not concourse_url.netloc or concourse_url.path != '':
    print(color("Url {} seems to be invalid. Please check your arguments.".format(url), fg='red'))
    exit(1)

session = requests.Session()
# print("Login status is {}".format(login_response.status_code))
builds_url = '{}/api/v1/teams/main/pipelines/{}/jobs/{}/builds'.format(url, pipeline, job)
# build_params = {'limit': build_count}
# build_response = session.get(builds_url, params=build_params)
build_response = session.get(builds_url)

builds = build_response.json()
sorted_builds = sorted(builds, key=itemgetter('id'), reverse=True)
failures = {}
# build = sorted_build[0]
completed_build_count = 0
failed_build_count = 0
for build in sorted_builds:
    # print(color("Checking build id {} ({}/{})".format(build['id'], job, build['name']), fg='yellow'))
    if build['status'] not in completed_build_states:
        continue
    completed_build_count += 1
    build_url = '{}{}'.format(url, build['api_url'])
    event_url = '{}/events'.format(build_url)

    event_response = session.get(event_url, stream=True, timeout=60)

    # print("Event Status is {}".format(event_response.status_code))
    event_output = ''
    build_status = 'unknown'

    event_client = sseclient.SSEClient(event_response)
    for event in event_client.events():
        if event.event == 'end':
            break
        if event.data:
            event_json = json.loads(event.data)
            event_data = event_json['data']
            if event_json['event'] == 'status':
                build_status = event_data['status']
                if build_status == 'succeeded':
                    break
            if event_json['event'] == 'log':
                event_output += event_data['payload']

            # print("***************************************************")
            # pprint.pprint("Event *{}* - {}".format(event.event,json.loads(event.data)))

    for line in event_output.splitlines():
        buildfailmatcher = re.search('BUILD FAILED|Test Failed!', line)
        if buildfailmatcher:
            failed_build_count += 1

        matcher = re.search('(\S+)\s*>\s*(\S+).*FAILED', line)
        if matcher:
            test_name = "{}.{}".format(matcher.group(1), matcher.group(2))
            if not failures.get(test_name):
                failures[test_name] = [build]
            else:
                failures[test_name].append(build)
            # print("Failure information: {} - {}".format(matcher.group(1), matcher.group(2)))

    # print("Results: Job status is {}".format(build_status))
    if completed_build_count == build_count:
        break
    time.sleep(2)

# pprint.pprint(failures)
if failed_build_count > 0:
    print(color("***********************************************************************************", fg='yellow'))
    print(" Overall build success rate: ", color("{}%".format((completed_build_count - failed_build_count)*100/completed_build_count), fg='blue'))
    print(color("***********************************************************************************", fg='yellow'))
    if failures:
        for failure in failures.keys():
            count = len(failures[failure])
            print(color("{}: ".format(failure), fg='cyan'),
                  color("{} failures".format(count), fg='red'),
                  color("({}% success rate)".format(((completed_build_count - count)/completed_build_count) * 100), fg='blue'))
            for build in failures[failure]:
                print(color("  Failed build {} ".format(build['name']), fg='red'), color("at {}/teams/{}/pipelines/{}/jobs/{}/builds/{}".format(url, build['team_name'],build['pipeline_name'], build['job_name'], build['name']), fg='magenta', style='bold'))
else:
    print(color("No failures! 100% success rate", fg='green',style='bold'))
