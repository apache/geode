import yaml
from jinja2 import Environment, FileSystemLoader

if __name__ == "__main__":
    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template('jinja.template.yml')

    variablesFromYml = open('jinja.variables.yml', 'r')
    variables = yaml.load(variablesFromYml)
    variablesFromYml.close()

    print(variables)
    file = open('generated-pipeline.yml', 'w')
    if variables is None:
        file.write(template.render())
    else:
        file.write(template.render(variables))
    file.close()
