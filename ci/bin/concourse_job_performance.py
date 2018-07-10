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

import argparse
import json
import logging
import re
from operator import itemgetter
from urllib.parse import urlparse

import requests
import sseclient
from colors import color
from tqdm import tqdm


def main(url, pipeline, job, build_count, team, max_fetch_count, authorization_cookie, thread_count):
    session = requests.Session()
    builds = get_builds(authorization_cookie, job, max_fetch_count, pipeline, session, team, url)
    builds_to_analyze = get_builds_to_analyze(build_count, builds, max_fetch_count)

    failures = {}
    completed_build_count = 0
    failed_build_count = 0

    for build in tqdm(builds_to_analyze):
        completed_build_count += 1
        build_url = '{}/api/v1/builds/{}'.format(url, build['id'])
        event_url = '{}/events'.format(build_url)

        event_response = session.get(event_url, cookies=authorization_cookie, stream=True, timeout=60)

        logging.debug("Event Status is {}".format(event_response.status_code))
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

                logging.debug("***************************************************")
                logging.debug("Event *{}* - {}".format(event.event, json.loads(event.data)))

        for line in event_output.splitlines():
            failed_build_count = check_line_for_failure(build, failed_build_count, failures, line)

        logging.debug("Results: Job status is {}".format(build_status))

    present_results(completed_build_count, failed_build_count, failures, url)


def present_results(completed_build_count, failed_build_count, failures, url):
    if failed_build_count > 0:
        print(color("***********************************************************************************", fg='yellow'))
        print(" Overall build success rate: ",
              color("{}%".format((completed_build_count - failed_build_count) * 100 / completed_build_count),
                    fg='blue'))
        print(color("***********************************************************************************", fg='yellow'))
        if failures:
            for failure in failures.keys():
                count = len(failures[failure])
                print(color("{}: ".format(failure), fg='cyan'),
                      color("{} failures".format(count), fg='red'),
                      color(
                          "({}% success rate)".format(((completed_build_count - count) / completed_build_count) * 100),
                          fg='blue'))
                for build in failures[failure]:
                    print(color("  Failed build {} ".format(build['name']), fg='red'),
                          color("at {}/teams/{}/pipelines/{}/jobs/{}/builds/{}".format(url,
                                                                                       build['team_name'],
                                                                                       build['pipeline_name'],
                                                                                       build['job_name'],
                                                                                       build['name']),
                                fg='magenta', style='bold'))
    else:
        print(color("No failures! 100% success rate", fg='green', style='bold'))


def check_line_for_failure(build, failed_build_count, failures, line):
    build_fail_matcher = re.search('BUILD FAILED|Test Failed!', line)
    if build_fail_matcher:
        failed_build_count += 1
    matcher = re.search('(\S+)\s*>\s*(\S+).*FAILED', line)
    if matcher:
        test_name = "{}.{}".format(matcher.group(1), matcher.group(2))
        if not failures.get(test_name):
            failures[test_name] = [build]
        else:
            failures[test_name].append(build)
        logging.debug("Failure information: {} - {}".format(matcher.group(1), matcher.group(2)))
    return failed_build_count


def get_builds_to_analyze(build_count, builds, max_fetch_count):
    sorted_builds = sorted(builds, key=itemgetter('id'), reverse=True)
    completed_build_states = ['succeeded', 'failed']
    builds_to_analyze = [b for b in sorted_builds if b['status'] in completed_build_states]
    if len(builds_to_analyze) < build_count:
        raise RuntimeError(
            "On the provided fetch limit {}, only {} jobs are completed, less than the desired {}".format(
                max_fetch_count, len(builds_to_analyze), build_count))
    return builds_to_analyze[:build_count]


def get_builds(authorization_cookie, job, max_fetch_count, pipeline, session, team, url):
    builds_url = '{}/api/v1/teams/{}/pipelines/{}/jobs/{}/builds'.format(url, team, pipeline, job)
    build_params = {'limit': max_fetch_count}
    build_response = session.get(builds_url, cookies=authorization_cookie, params=build_params)
    return build_response.json()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('url',
                        help="URL to Concourse.",
                        type=lambda s: str(s).rstrip('/'))
    parser.add_argument('pipeline',
                        help="Name of pipeline.",
                        type=str)
    parser.add_argument('job',
                        help="Name of job.",
                        type=str)
    parser.add_argument('limit',
                        help="Number of completed jobs to examine.",
                        type=int,
                        nargs='?',
                        default=50)
    parser.add_argument('team',
                        help="Team to which the provided pipeline belongs.",
                        type=str,
                        nargs='?',
                        default="main")
    parser.add_argument('fetch',
                        help="Limit size of initial build-status page.",
                        type=int,
                        nargs='?',
                        default=100)
    parser.add_argument('--cookie-token',
                        help="If authentication is required, provide your ATC-Authorization cookie's token here.",
                        type=lambda t: {u'ATC-Authorization': '"Bearer {}"'.format(t)})

    args = parser.parse_args()

    #
    # if len(sys.argv) not in (5, 6, 7):
    #     print("Usage: {} <concourse url> <pipeline> <job> <count> <team=main>".format(os.path.basename(sys.argv[0])))
    #     exit(1)
    #
    # _url = sys.argv[1].rstrip('/')
    # _pipeline = sys.argv[2]
    # _job = sys.argv[3]
    # _build_count = int(sys.argv[4])
    # _team = sys.argv[5] if len(sys.argv) >= 6 else "main"
    # _max_fetch_count = sys.argv[6] if len(sys.argv) >= 7 else 100

    # Validation
    concourse_url = urlparse(args.url)
    if not concourse_url.scheme or not concourse_url.netloc or concourse_url.path != '':
        print(color("Url {} seems to be invalid. Please check your arguments.".format(args.url), fg='red'))
        exit(1)

    assert args.fetch >= args.limit, "Fetching fewer jobs than you will analyze is pathological."

    _thread_count = 5
    main(args.url.rstrip('/'), args.pipeline, args.job, args.limit, args.team, args.fetch
         , args.cookie_token, _thread_count)
