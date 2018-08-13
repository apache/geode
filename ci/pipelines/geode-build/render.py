import yaml
from jinja2 import Environment, FileSystemLoader
import os

if __name__ == "__main__":
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('jinja.template.yml')

    variablesFromYml = open('jinja.variables.yml', 'r')
    variables = yaml.load(variablesFromYml)
    variablesFromYml.close()

    variables['repository']['branch'] = os.environ['GEODE_BRANCH']
    variables['repository']['fork'] = os.environ['GEODE_FORK']

    print(variables)

    file = open('generated-pipeline.yml', 'w')
    file.write(template.render(variables))
    file.close()
