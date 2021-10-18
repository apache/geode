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

"""This script performs a multi-pass render of a Jinja templated file.
See the argparse help for details."""
import argparse
import jinja2.exceptions
import logging
import os
import pprint
import yaml
from itertools import combinations
from jinja2 import Environment, FileSystemLoader, Undefined
from typing import List, Dict

RENDER_PASS_LIMIT = 5


class RaiseExceptionIfUndefined(Undefined):
    """If a variable is not defined, do not interpolate it to be an empty string per Jinja's default behavior.
    Rather, the render script should fail vocally.

    The actual failure must be raised in __str__, when the renderer attempts to interpolate this value.
    """

    def __str__(self):
        raise jinja2.exceptions.UndefinedError(f"Could not find variable definition for '{self._undefined_name}'.")


def render_template(template_file: str,
                    variables_files: List[str], environment_dirs: List[str],
                    command_line_variable_options: List[Dict],
                    print_rendered: bool, output_file: str):
    env = get_environment(environment_dirs, template_file)
    variables = determine_variables(command_line_variable_options, variables_files)
    template = env.get_template(os.path.basename(template_file))
    rendered_template = multipass_render(template, variables, env)

    if print_rendered:
        print(rendered_template)

    logging.debug(f"Writing to output '{output_file}'")
    with open(output_file, 'w') as pipeline_file:
        pipeline_file.write(rendered_template)


def get_environment(environment_dirs, template_file):
    """Gets the Jinja Environment for the renderer.
    Uses a FileSystemLoader with the specified environment dirs, as well as the directory containing the template_file.
    Raises an error if any variable cannot be resolved."""
    environment_searchpath = [get_absolute_dirname(template_file)] + environment_dirs
    logging.debug(f"Environment search path:\n{pprint.pformat(environment_searchpath)}")
    env = Environment(loader=FileSystemLoader(environment_searchpath),
                      extensions=['jinja2.ext.do'],
                      undefined=RaiseExceptionIfUndefined,
                      )
    return env


def multipass_render(template, variables, environment):
    f"""Performs a multiple-pass render of the template file, allowing variables to reference other variables.
    Performs at most RENDER_PASS_LIMIT (={RENDER_PASS_LIMIT}) passes.
    """
    this_template = template

    for i in range(1, RENDER_PASS_LIMIT + 1):
        logging.debug(f"Performing render pass {i}...")

        this_template_rendered = this_template.render(variables)
        if "{{" not in this_template_rendered:
            return this_template_rendered
        this_template = environment.from_string(this_template_rendered)

    raise RuntimeError(f"Variables not eliminated from template after {RENDER_PASS_LIMIT} passes."
                       f"  Please verify that template variables references to not recurse.")


def determine_variables(command_line_variable_options: List[Dict], variables_files):
    """Merges variables from each specified variable file and any command-line specified variables,
     warning when conflicts exist."""
    # For safety purposes, we first generate a list of tuples for each specified variable,
    # where each tuple is of the form (source, key, value).
    variable_list = [(source, key, value)
                     for source in variables_files
                     for key, value in get_variables_from_file(source).items()]
    # Add any command-line variables last to give them the highest priority
    variable_list.extend([
        ("command-line option", k, v)
        for cmd_opt in command_line_variable_options
        for k, v in cmd_opt.items()
    ])
    # Scan though the variable set to make sure there are no conflicts
    for (s1, k1, v1), (s2, k2, v2) in combinations(variable_list, 2):
        if k1 == k2 and v1 == v2:
            logging.info(f"Key value pair ({k1}: {v1}) is duplicated in variable files {s1} and {s2}")
        if k1 == k2 and v1 != v2:
            logging.warning(f"Conflicting variable specifications provided.\n"
                            f"Key value pair from {s1}:\n"
                            f"  {k1}: {v1}\n"
                            f"has been overwritten by {s2}:\n"
                            f"  {k2}: {v2}")

    # Here as above, we maintain load order so that later-specified variable files will overwrite earlier-specified.
    variables = {key: value for _, key, value in variable_list}

    logging.debug(f"Variables:\n{pprint.pformat(variables)}\n")

    return variables


def get_variables_from_file(absolute_var_file):
    logging.debug(f"Loading variables from file {absolute_var_file}")
    with open(absolute_var_file, 'r') as variablesFromYml:
        return yaml.safe_load(variablesFromYml)


def get_absolute_dirname(some_path):
    """If the path given is a directory, returns the absolute path to that directory.
    If the path given is a file, returns the absolute path to the directory containing that file.
    Errors if the path does not exist or could not otherwise resolve."""
    absolute_path = os.path.abspath(some_path)
    if os.path.isfile(absolute_path):
        return os.path.abspath(os.path.dirname(os.path.abspath(absolute_path)))

    if os.path.isdir(absolute_path):
        return os.path.abspath(absolute_path)

    if not os.path.exists(absolute_path):
        raise FileNotFoundError(f"Path '{absolute_path}' does not exist.")

    raise ValueError(f"Something strange happened attempting to resolve the absolute dirname of '{absolute_path}'")


if __name__ == '__main__':
    # All files / directories specified are converted to absolute paths by the ArgParser
    parser = argparse.ArgumentParser()
    parser.add_argument("template",
                        help="Jinja template file.",
                        type=lambda s: os.path.abspath(s)
                        )
    parser.add_argument("--variable-file",
                        help="Jinja variables file.  Multiple files can be provided."
                             "  You will be warned if any definitions collide."
                             "  Preference will be given to the file specified last.",
                        nargs='+',
                        default=[],
                        type=lambda s: os.path.abspath(s))
    parser.add_argument("--environment",
                        help="Additional directories to pass to the Jinja environment, e.g.,"
                             " a 'shared/' directory containing files referenced by name,"
                             " e.g., '{%% from shared_jinja.yml import x with context %%}'",
                        nargs="+",
                        default=[],
                        type=get_absolute_dirname
                        )
    parser.add_argument("--raw-variable",
                        help="YML or JSON string defining additional variables."
                             "  This option has preference greater than variable files.",
                        nargs="+",
                        default=[],
                        type=lambda s: yaml.safe_load(s))
    parser.add_argument("-o",
                        "--output",
                        help="Output file",
                        default=f"generated-pipeline.yml",
                        type=lambda s: os.path.abspath(s))
    parser.add_argument("--print-to-console",
                        help="Prints the rendered file before writing to --output",
                        action="store_true")
    parser.add_argument("--debug",
                        help="It's debug.  If you have to ask, you'll never know.",
                        action="store_true")
    parser.add_argument("--info",
                        help="It's info.  It's like a lazy --debug.",
                        action="store_true")

    _args = parser.parse_args()

    if _args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    elif _args.info:
        logging.getLogger().setLevel(logging.INFO)
    else:
        logging.getLogger().setLevel(logging.WARNING)

    # Preprocess file arguments so that we only deal with absolute paths.
    render_template(_args.template,
                    _args.variable_file,
                    _args.environment,
                    _args.raw_variable,
                    _args.print_to_console,
                    _args.output)
