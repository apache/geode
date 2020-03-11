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

import argparse
import itertools
import json
import logging
import multiprocessing
import os
import re
import threading
from multiprocessing.dummy import Pool
from operator import itemgetter
from typing import List
from urllib.parse import urlparse

import requests
# from sseclient-py
import sseclient
# from ansicolors
from colors import color
from tqdm import tqdm
import yaml

TEST_FAILURE_REGEX = re.compile('(\S+)\s*>\s*(\S+).*FAILED')

YELLOW_STARS_SEPARATOR = color("***********************************************************************************",
                               fg='yellow')


class SingleFailure:
    """Information container class for detected failures."""
    def __init__(self, class_name, method, build_json):
        self.class_name = class_name
        self.method = method
        self.build_json = build_json

    def __str__(self):
        return f"Failure({self.class_name}, {self.method}, ({self.build_json['name']} ...))"


def main(url, team, pipeline, job, number_of_builds, authorization_cookie, threaded):

    cookie = get_cookie(url)
    authorization_cookie = {u'skymarshal_auth0': cookie}
    builds = get_builds_summary_sheet(url, team, pipeline, job, number_of_builds+10, authorization_cookie)

    build_to_examine = get_builds_to_examine(builds, number_of_builds)
    expected_failed_builds = [int(b['name']) for b in build_to_examine if b['status'] in ['failed', 'succeeded']]
    expected_failed_builds_count = len(expected_failed_builds)
    logging.info(f"Expecting {expected_failed_builds_count} runs to have failure strings: {expected_failed_builds}")

    long_list_of_failures = aggregate_failure_information(authorization_cookie, build_to_examine, threaded, url)

    failure_url_base = f"{url}/teams/{team}/pipelines/{pipeline}/jobs/{job}/builds/"

    print_results(len(build_to_examine), expected_failed_builds, long_list_of_failures, url, failure_url_base)


def get_cookie(url):
    home = os.environ['HOME']
    data = yaml.load(open(f"{home}/.flyrc"))
    for target in data["targets"]:
        api = data["targets"][target]["api"]
        if api == url:
            cookie = "\"Bearer " + data["targets"][target]["token"]["value"] + "\""
            return cookie
    return ""


def aggregate_failure_information(authorization_cookie, build_to_examine, threaded, url) -> List[SingleFailure]:
    progress_bar = tqdm(total=len(build_to_examine), desc="Build pages examined")

    def build_examiner(build):
        """ Convenience method for thread pool / list comprehension consumption below."""
        return examine_build_and_update_progress(authorization_cookie, build, url, progress_bar)

    if threaded:
        # Number of CPUs is not necessarily number of CPUs available.
        # Since it's a minor thing, we'll just ask for half.
        n_cpus = multiprocessing.cpu_count() - 2
        logging.info(f"Using {n_cpus} threads to make queries.")

        pool = Pool(n_cpus)
        list_of_list_of_failures = pool.map(build_examiner, build_to_examine)
    else:
        list_of_list_of_failures = [build_examiner(build) for build in build_to_examine]

    data = list(itertools.chain(*list_of_list_of_failures))
    return data


def examine_build_and_update_progress(authorization_cookie, build, url, progress_bar) -> List[SingleFailure]:
    my_pid = threading.current_thread()
    logging.debug(f"[{my_pid}] Examining {build['name']}")
    failures = examine_build(authorization_cookie, build, url)
    progress_bar.update(1)
    return failures


def examine_build(authorization_cookie, build, url) -> List[SingleFailure]:
    session = requests.Session()

    event_response = get_event_response(authorization_cookie, build, session, url)
    logging.debug("Event Status is {}".format(event_response.status_code))

    build_status, event_output = assess_event_response(event_response)
    this_build_failures = assess_event_output_for_failure(build, event_output)
    logging.debug("Results: Job status is {}".format(build_status))

    return this_build_failures


def assess_event_output_for_failure(build, event_output) -> List[SingleFailure]:
    all_failures = []
    for line in event_output.splitlines():
        """Returns true if no failure is found, false if a failure is found."""
        test_failure_matcher = TEST_FAILURE_REGEX.search(line)
        if test_failure_matcher:
            class_name, method_name = test_failure_matcher.groups()
            this_failure = SingleFailure(class_name, method_name, build)
            all_failures.append(this_failure)
            logging.debug(f"Failure identified, {this_failure}")
    return all_failures


def assess_event_response(event_response):
    event_outputs = []
    build_status = 'unknown'
    event_client = sseclient.SSEClient(event_response)

    for event in event_client.events():
        event_json = json.loads(event.data if event.data else "{}")
        build_status = (event_json['data']['status']
                        if event_json.get('event', 'not-a-status-event') == 'status'
                        else build_status)
        if event.event == 'end' or event_json['event'] == 'status' and build_status == 'succeeded':
            return build_status, ''.join(event_outputs)
        elif event.data:
            if event_json['event'] == 'status':
                if build_status == 'succeeded':
                    return build_status, ''.join(event_outputs)
            elif event_json['event'] == 'log':
                event_outputs.append(event_json['data']['payload'])

            logging.debug("***************************************************")
            logging.debug("Event *{}* - {}".format(event.event, json.loads(event.data)))
    print("Current status:", build_status)
    print("Current aggregated event output stream:", ''.join(event_outputs))
    raise RuntimeError("An event response has no 'end' event and no 'succeeded'.  See the failing output above.")


def get_event_response(authorization_cookie, build, session, base_url):
    event_url = f'{base_url}{build["api_url"]}/events'
    event_response = session.get(event_url, cookies=authorization_cookie, stream=True, timeout=60)
    if event_response.status_code != 200:
        raise IOError(f"Event response query to endpoint < {event_url} > returned code {event_response.status_code}.")
    return event_response


def print_results(n_builds_analyzed, expected_failed_builds, long_list_of_failures, url, failure_url_base):
    if long_list_of_failures or expected_failed_builds:
        print_failures(n_builds_analyzed, expected_failed_builds, long_list_of_failures, url, failure_url_base)
    else:
        print(color("No failures! 100% success rate", fg='green', style='bold'))


def print_failures(completed, expected_failed_builds, long_list_of_failures, url, failure_url_base):
    print(YELLOW_STARS_SEPARATOR)
    print_success_rate_and_expectation_warning(completed, expected_failed_builds, long_list_of_failures,
                                               failure_url_base)
    print_failures_in_classes_that_share_method_names(completed, long_list_of_failures)
    print_class_and_test_failures_with_links(completed, long_list_of_failures, url)
    print_list_of_failures(long_list_of_failures, url)
    # Then highlight any build runs that failed hard / timed out.


def print_failures_in_classes_that_share_method_names(completed, long_list_of_failures):
    classes_with_a_particular_failure = {}
    for failure in long_list_of_failures:
        test_name = failure.method
        class_name = failure.class_name
        if test_name not in classes_with_a_particular_failure:
            classes_with_a_particular_failure[test_name] = {}
            classes_with_a_particular_failure[test_name][class_name] = 1
        elif class_name not in classes_with_a_particular_failure[test_name]:
            classes_with_a_particular_failure[test_name][class_name] = 1
        else:
            classes_with_a_particular_failure[test_name][class_name] += 1
    # Filter to only those methods that are found in multiple classes
    classes_with_a_particular_failure = {m: cl for m, cl in classes_with_a_particular_failure.items() if len(cl) > 1}
    if classes_with_a_particular_failure:
        print("The following test methods see failures in more than one class.  There may be a failing *TestBase class")
        for test_name, classes_and_counts in classes_with_a_particular_failure.items():
            formatted_class_output = ""
            total_failure_count = sum(count for _, count in classes_and_counts.items())
            formatted_test_name_with_success_rate = success_rate_string(f"*.{test_name}", total_failure_count, None)
            for failing_class, failure_count in classes_and_counts.items():
                formatted_class_output += "\n  "
                class_name_without_package = failing_class.split('.')[-1]
                formatted_class_output += success_rate_string(class_name_without_package, failure_count, completed)

            print(f"\n{formatted_test_name_with_success_rate}:{formatted_class_output}")
        print()
        print(YELLOW_STARS_SEPARATOR)
        print()


def print_class_and_test_failures_with_links(completed, long_list_of_failures, url):
    failed_classes = set(failure.class_name for failure in long_list_of_failures)
    for this_class_name in failed_classes:
        this_class_failures = [failure for failure in long_list_of_failures if failure.class_name == this_class_name]
        failed_tests = set(failure.method for failure in this_class_failures)
        class_build_failures_count = sum(1 for _ in {failure.build_json['name'] for failure in this_class_failures})

        print(os.linesep + success_rate_string(this_class_name, class_build_failures_count, completed) + os.linesep)

        for this_test_method_name in failed_tests:
            this_test_method_failures = [failure for failure in this_class_failures
                                         if failure.method == this_test_method_name]

            for this_failure in this_test_method_failures:
                build = this_failure.build_json
                failed_build_url = (f"{url}/teams/{build['team_name']}/pipelines/"
                                    f"{build['pipeline_name']}/jobs/{build['job_name']}/builds/{build['name']}")

                print("         " + this_test_method_name + "       " + failed_build_url)


def print_list_of_failures(long_list_of_failures, url):
    print(os.linesep * 3)
    for this_failure in long_list_of_failures:
        build = this_failure.build_json
        failed_build_url = (f"{url}/teams/{build['team_name']}/pipelines/"
                                 f"{build['pipeline_name']}/jobs/{build['job_name']}/builds/{build['name']}")
        print(this_failure.class_name+"."+this_failure.method+", "+failed_build_url)


def success_rate_string(identifier, failure_count, total_count):
    """Only prints percentage if total_count is not None"""
    return " ".join((color(f"{identifier}: ", fg='cyan'),
                     color(f"{failure_count} failures", fg='red'),
                     ""
                     if total_count is None else
                     color(f"({((total_count - failure_count) / total_count) * 100:6.3f}% success rate)", fg='blue')))


def print_success_rate_and_expectation_warning(completed, expected_failed_builds, long_list_of_test_failures,
                                               failure_url_base):
    # "Build failures" will refer to jobs that went red
    # "Test failures" will refer to jobs whose output matched our parsing regex

    build_failure_count = len(expected_failed_builds)
    test_failure_set = {int(failure.build_json['name']) for failure in long_list_of_test_failures}
    test_failure_count = sum(1 for _ in test_failure_set)

    # Matched failure string (in long_list) but not an expected failure (box went red)
    test_failure_not_build_failure = [name for name in test_failure_set if name not in expected_failed_builds]

    # Expected failures (box went red) but did not match a failure string (in long_list)
    build_failure_not_test_failure = [name for name in expected_failed_builds if name not in test_failure_set]

    rate = (completed - test_failure_count) * 100 / completed

    print(" Overall build success rate:",
          color(f"{rate:.5f}% ({completed - test_failure_count} of {completed})", fg='blue'))

    if build_failure_not_test_failure:
        print(color(f'>>>>> {build_failure_count} jobs "went red," '
                    f'but only {test_failure_count} were detected test failures. <<<<<',
                    fg='red', style='bold'))
        print(f"Maybe you have some timeouts or other issues please manually inspect the following builds:")
        for build_failure in build_failure_not_test_failure:
            print(f"  {failure_url_base}{build_failure}")
    if test_failure_not_build_failure:
        print(color(f'>>>>> OH NO!  A test failure was detected, but the job "went green" anyway!! <<<<',
                    fg='red', style='bold'))
        print(f"Please manually inspect the following builds:")
        for test_failure in test_failure_not_build_failure:
            print(f"  {failure_url_base}{test_failure}")
    print(YELLOW_STARS_SEPARATOR)
    print()


def get_builds_to_examine(builds, build_count):
    """
    :param builds: Build summary JSON
    :param build_count: number of completed builds to return.  Return all if 0
    """
    # possible build statuses:
    statuses = ['succeeded', 'failed', 'aborted', 'errored', 'pending', 'started']
    succeeded, failed, aborted, errored, pending, started = sieve(builds, lambda b: b['status'], *statuses)
    completed_builds = succeeded + failed
    completed_builds.sort(key=itemgetter('id'), reverse=True)
    if build_count == 0:
        count_of_builds_to_return = len(completed_builds)
    else:
        count_of_builds_to_return = build_count

    builds_to_analyze = completed_builds[:count_of_builds_to_return] if count_of_builds_to_return else completed_builds

    if len(builds_to_analyze) == 0:
        logging.info("No completed builds to examine")
    elif len(builds_to_analyze) == 1:
        logging.info(f"1 completed build to analyze - {builds_to_analyze[0]['name']}")
    else:
        first_build = builds_to_analyze[-1]['name']
        last_build = builds_to_analyze[0]['name']
        logging.info(f"{len(started)} completed builds to examine, ranging "
                     f"{first_build} - {last_build}: {list_and_sort_by_name(builds_to_analyze)}")

    logging.debug(f"{len(aborted)} aborted builds in examination range: {list_and_sort_by_name(aborted)}")
    logging.debug(f"{len(errored)} errored builds in examination range: {list_and_sort_by_name(errored)}")
    logging.debug(f"{len(pending)} pending builds in examination range: {list_and_sort_by_name(pending)}")
    logging.debug(f"{len(started)} started builds in examination range: {list_and_sort_by_name(started)}")
    returnable = (len(failed) + len(errored) + len(succeeded) + len(pending) + len(aborted))
    if returnable == 0:
        raise IOError(f"There are 0 completed jobs ")

    failures_in_analysis_range = list_and_sort_by_name([build for build in builds_to_analyze
                                                        if build['status'] == 'failed'])
    logging.info(f"{len(failures_in_analysis_range)} expected failures in analysis range: {failures_in_analysis_range}")
    return builds_to_analyze


def list_and_sort_by_name(builds):
    return sorted([int(b['name']) for b in builds], reverse=True)


def get_builds_summary_sheet(url, team, pipeline, job,  number_of_builds, authorization_cookie):
    session = requests.Session()
    if number_of_builds == 0:
        # Snoop the top result's name to discover the number of jobs that have been queued.
        snoop = get_builds_summary_sheet(url, team, pipeline, job,  1, authorization_cookie)
        number_of_builds = int(snoop[0]['name'])
        logging.info(f"Snooped: fetching a full history of {number_of_builds} builds.")

    builds_url = '{}/api/v1/teams/{}/pipelines/{}/jobs/{}/builds'.format(url, team, pipeline, job)
    build_params = {'limit': number_of_builds}
    logging.info(f"fetching the last {number_of_builds} builds.")
    build_response = session.get(builds_url, cookies=authorization_cookie, params=build_params)
    if build_response.status_code != 200:
        raise IOError(f"Initial build summary query to {builds_url} returned status code {build_response.status_code}.")
    return build_response.json()


def sieve(iterable, inspector, *keys):
    """Separates @iterable into multiple lists, with @inspector(item) -> k for k in @keys defining the separation.
    e.g., sieve(range(10), lambda x: x % 2, 0, 1) -> [[evens], [odds]]
    """
    s = {k: [] for k in keys}
    for item in iterable:
        k = inspector(item)
        if k not in s:
            raise KeyError(f"Unexpected key <{k}> found by inspector in sieve.")
        s[inspector(item)].append(item)
    return [s[k] for k in keys]


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    # Required args
    parser.add_argument('url',
                        help="URL to Concourse.",
                        type=lambda s: s.rstrip('/'))
    parser.add_argument('pipeline',
                        help="Name of pipeline.",
                        type=str)
    parser.add_argument('job',
                        help="Name of job.",
                        type=str)
    parser.add_argument('--number-of-builds',
                        help="The number of builds to fetch.  [Default=0] aka All.",
                        type=int,
                        nargs='?',
                        default=0)
    # Optional args
    parser.add_argument('--team',
                        help='Team to which the provided pipeline belongs.  [Default="main"]',
                        type=str,
                        nargs='?',
                        default="main")
    parser.add_argument('--cookie-token',
                        help='If authentication is required (e.g., team="staging"), '
                             "provide your skymarshal_auth0 cookie's token here.  "
                             "Unfortunately, this is currently done by logging in via a web browser, "
                             "inspecting your cookies, and pasting it here.",
                        type=lambda t: {u'skymarshal_auth0':
                                        '"Bearer {}"'.format(t if not t.startswith("Bearer ") else t[7:])})
    parser.add_argument('--threaded',
                        help="Use multiple cores to hasten api requests.",
                        action="store_true")
    parser.add_argument('--verbose',
                        help="Enable info logging.",
                        action="store_true")
    parser.add_argument('--debug',
                        help="Enable debug logging.  Implies --verbose",
                        action="store_true")

    args = parser.parse_args()

    # Validation
    concourse_url = urlparse(args.url)
    if not concourse_url.scheme or not concourse_url.netloc or concourse_url.path != '':
        print(color("Url {} seems to be invalid. Please check your arguments.".format(args.url), fg='red'))
        exit(1)

    # # Examination limit should be less than fetch limit, or either/both should be set to 0 for full analysis
    # if args.starting_build and args.n and args.starting_build < args.number_of_builds:
    #     raise AssertionError("Fetching fewer jobs than you will analyze is pathological.")

    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    elif args.verbose:
        logging.getLogger().setLevel(logging.INFO)

    main(args.url, args.team, args.pipeline, args.job, args.number_of_builds, args.cookie_token,
         args.threaded)
