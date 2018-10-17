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
import logging

import yaml
from jinja2 import Environment, FileSystemLoader, Undefined
import jinja2.exceptions
import os
import subprocess

def main(template_dir_name, geode_fork, geode_branch, upstream_fork, repository_public):

    script_dir_ = os.path.dirname(os.path.abspath(__file__))
    shared_dir_ = os.path.join(script_dir_, 'shared')
    variables_file = os.path.join(shared_dir_, 'jinja.variables.yml')
    template_file = os.path.join(template_dir_name, 'jinja.template.yml')
    output_file_ = os.path.join(script_dir_, template_dir_name, 'generated-pipeline.yml')

    cwd_ = os.getcwd()
    # Jinja2 template loaded does not allow looking UP from the paths below, so be verbose to make sure we can include
    # shared/common templates
    env = Environment(loader=FileSystemLoader([cwd_, script_dir_]),
      extensions=['jinja2.ext.do']
    )
    template = env.get_template(template_file)

    with open(variables_file, 'r') as variablesFromYml:
        variables = yaml.load(variablesFromYml)

    variables['repository']['branch'] = geode_branch
    variables['repository']['fork'] = geode_fork
    variables['repository']['upstream_fork'] = upstream_fork
    variables['repository']['public'] = repository_public

    # Use the one-true-way to truncate fork and branches, trimming the Python bytestream characters from the front and
    # back. If this is too ugly, then convert the BASH functions into python files, and call that Python from the
    # deploy_XYZ.sh scripts
    variables['repository']['sanitized_branch'] = subprocess.run(['bash', '-c', '. ' + script_dir_ + '/shared/utilities.sh; getSanitizedBranch ' + geode_branch], stdout=subprocess.PIPE).stdout.decode('utf-8')[:-1]
    variables['repository']['sanitized_fork'] = subprocess.run(['bash', '-c', '. ' + script_dir_ + '/shared/utilities.sh; getSanitizedFork ' + geode_fork], stdout=subprocess.PIPE).stdout.decode('utf-8')[:-1]

    logging.debug(f"Variables = {variables}")

    logging.info(template.render(variables))
    with open(output_file_, 'w') as pipeline_file:
        pipeline_file.write(template.render(variables))


class RaiseExceptionIfUndefined(Undefined):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __str__(self):
        raise jinja2.exceptions.UndefinedError("Interpolating undefined variables has been disabled.")

    def __iter__(self):
        raise jinja2.exceptions.UndefinedError("Interpolating undefined variables has been disabled.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("template_dir_name", help="Directory containing Jinja template file.")
    parser.add_argument("geode_fork", help="Name of the git fork")
    parser.add_argument("geode_branch", help="Branch against which pull-requests are made")
    parser.add_argument("upstream_fork", help="Name of the upstream into which this fork would merge")
    parser.add_argument("repository_public", help="Is repository public?")
    # parser.add_argument("variables", help="Jinja variables file.")
    # parser.add_argument("output", help="Output target.")
    parser.add_argument("--debug", help="It's debug.  If you have to ask, you'll never know.", action="store_true")

    _args = parser.parse_args()

    if _args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logging.debug(f"cwd: {os.getcwd()}")

    main(_args.template_dir_name, _args.geode_fork, _args.geode_branch, _args.upstream_fork, _args.repository_public)

