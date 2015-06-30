import os.path
import requests
import logging

from .abstract_appserver import AbstractAppserver
from fabric.api import *
from fabric.context_managers import *
from fabric.contrib.files import *
from fabric.utils import *
from datetime import *


class TcServer(AbstractAppserver):

    def __init__(self, env):
        AbstractAppserver.__init__(self, env)

    def start(self):
        with cd(self.home_dir):
            x = run('./tcruntime-ctl.sh {0} status'.format(self.instance_name))

            if x.find('RUNNING as') < 0:
                run('rm -f {0}/logs/*'.format(self.instance_name))
                run('./tcruntime-ctl.sh {0} start'.format(self.instance_name))

    def stop(self):
        with cd(self.home_dir), path('.'), quiet():
            run('./tcruntime-ctl.sh {0} stop'.format(self.instance_name))

    def kill(self):
        with cd(self.home_dir):
            run('kill -9 `cat {0}/logs/tcserver.pid`'.format(self.instance_name))

    def destroy_instance(self):
        with cd(self.home_dir):
            run('rm -rf {0}'.format(self.instance_name))

    def cleanup_templates(self):
        with cd(self.home_dir):
            run('rm -rf {0}/gemfire-*'.format(self.artifact_location))

    def setup_redeploy(self, flavor):
        with cd(self.home_dir):
            setenv_sh = '{0}/bin/setenv.sh'.format(self.instance_name)
            if not contains(setenv_sh, 'gemfire.loadClassOnEveryDeserialization'):
                sed(setenv_sh, 'JVM_OPTS="', '&-Dgemfire.loadClassOnEveryDeserialization=true ')

            if flavor == 'gemfire-p2p':
                catalina_props = '{0}/conf/catalina.properties'.format(self.instance_name)
                sed(catalina_props, 'prefer.deserialized.form=.*', 'prefer.deserialized.form=false')
                

    def restore_redeploy(self, flavor):
        with cd(self.home_dir):
            setenv_sh = '{0}/bin/setenv.sh'.format(self.instance_name)
            if contains(setenv_sh, 'gemfire.loadClassOnEveryDeserialization'):
                sed(setenv_sh, '-Dgemfire.loadClassOnEveryDeserialization=[^ ]* ', '')

            if flavor == 'gemfire-p2p':
                catalina_props = '{0}/conf/catalina.properties'.format(self.instance_name)
                sed(catalina_props, 'prefer.deserialized.form=.*', 'prefer.deserialized.form=true')
                

    def undeploy_webapp(self, host, context):
        url = 'http://{0}:{1}/manager/text/undeploy'.format(host, self.port)

        query_params = {
            'path' : context,
        }

        try:
            c = self.list_contexts(host, self.port)
            if context in c:
                r = requests.get(url,
                        params=query_params,
                        auth=requests.auth.HTTPBasicAuth('admin', 'admin')
                )

                if r.text.find('FAIL') >= 0:
                    error('Undeployement failed: {0}'.format(r.text))
                else:
                    logging.info(r.text)
            else:
                logging.info('Webapp {0} not found to undeploy'.format(context))
        except requests.exceptions.ConnectionError, e:
            # Perhaps the webserver is down so we remove it forcibly
            run('rm -rf {0}/{1}/webapps/{2}'.format(self.home_dir, self.instance_name, context))
            run('rm -f {0}/{1}/webapps/{2}.war'.format(self.home_dir, self.instance_name, context))


    def list_contexts(self, host, port):
        url = 'http://{0}:{1}/manager/text/list'.format(host, self.port)
        results = []

        r = requests.get(url, auth=requests.auth.HTTPBasicAuth('admin', 'admin'))
        for x in r.text.split('\n'):
            y = x.split(':')
            if len(y) > 1:
                results.append(y[0])

        return results



    def log_datestamp(self):
        return datetime.now().strftime('%Y-%m-%d')
