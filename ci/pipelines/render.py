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

def main(template_file, variables_file, output_file):
    # TODO Delete this TODO
    # Also TODO: Make the FileSystemLoader accept the script-dir, current-dir, and commons-dir more sensibly.
    env = Environment(loader=FileSystemLoader(['.', '..']), undefined=RaiseExceptionIfUndefined)
    template = env.get_template(template_file)

    with open(variables_file, 'r') as variablesFromYml:
        variables = yaml.load(variablesFromYml)

    variables['repository']['branch'] = os.environ['GEODE_BRANCH']
    variables['repository']['fork'] = os.environ['GEODE_FORK']

    logging.debug(f"Variables = {variables}")

    logging.info(template.render(variables))
    with open(output_file, 'w') as pipeline_file:
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
    parser.add_argument("template", help="Jinja template file.")
    parser.add_argument("variables", help="Jinja variables file.")
    parser.add_argument("output", help="Output target.")
    parser.add_argument("--debug", help="It's debug.  If you have to ask, you'll never know.", action="store_true")

    _args = parser.parse_args()

    if _args.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    logging.debug(f"cwd: {os.getcwd()}")

    main(_args.template, _args.variables, _args.output)

