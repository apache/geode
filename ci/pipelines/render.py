#!/usr/bin/env python3
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

