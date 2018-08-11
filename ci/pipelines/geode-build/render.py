import yaml
from jinja2 import Environment, FileSystemLoader
import sys

if __name__ == "__main__":
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('jinja.template.yml')

    variablesFromYml = open('jinja.variables.yml', 'r')
    variables = yaml.load(variablesFromYml)
    variablesFromYml.close()

    # Add repo variables from command line args
    variables['repository']['fork'] = sys.argv[1]
    variables['repository']['branch'] = sys.argv[2]

    print(variables)

    file = open('generated-pipeline.yml', 'w')
    file.write(template.render(variables))
    file.close()
