#!/usr/bin/env python3
import argparse

import yaml
from jinja2 import Environment, FileSystemLoader
import os

def main(template_file, variables_file, output_file):
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template(template_file)

    with open(variables_file, 'r') as variablesFromYml:
        variables = yaml.load(variablesFromYml)

    variables['repository']['branch'] = os.environ['GEODE_BRANCH']
    variables['repository']['fork'] = os.environ['GEODE_FORK']

    print(variables)

    with open(output_file, 'w') as pipeline_file:
        pipeline_file.write(template.render(variables))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--template", default="jinja.template.yml", help="Jinja template file.")
    parser.add_argument("--variables", default="jinja.variables.yml", help="Jinja variables file.")
    parser.add_argument("--output", default="generated-pipeline.yml", help="Output target.")

    args = parser.parse_args()

    main(args.template, args.variables, args.output)

