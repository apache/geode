import logging
import requests

from .factory import Factory
from .tcserver import TcServer
from .abstract_appserver import AbstractAppserver

from fabric.api import *
from fabric.context_managers import *
from fabric.contrib.files import *
from fabric.utils import *

class TcServerAppserver(TcServer):

    @staticmethod
    def container_type():
        return "tcserver_appserver"


    def __init__(self, env):
        AbstractAppserver.__init__(self, env)


    def create_instance(self, gemfire_home, template):
        with cd(self.home_dir), shell_env(JAVA_HOME=self.java_home):
            run('rm -rf {0}'.format(self.instance_name))
            run('./tcruntime-instance.sh create {0}'.format(self.instance_name))


    def customize_instance(self, resource_dir, flavor, webapp_jar_mode):
        tomcat_users = os.path.join(resource_dir, 'tomcat-users.xml')
        with cd(self.home_dir):
            run('cp {0}/lib/gemfire.jar {1}/lib'.format(env.gemfire_home, self.artifact_location))
            run('cp {0}/lib/antlr.jar {1}/lib/'.format(env.gemfire_home, self.artifact_location))
            put(env.json_jar, '{0}/lib/'.format(self.artifact_location))
            put(tomcat_users, '{0}/conf/tomcat-users.xml'.format(self.instance_name))

            if flavor == 'gemfire-cs':
                cache_xml = '{0}/conf/cache-server.xml'.format(self.artifact_location)
            else:
                cache_xml = '{0}/conf/cache-peer.xml'.format(self.artifact_location)

            if not contains(cache_xml, 'JsonFunction'):
                sed(cache_xml, '</cache>',
                        '<initializer><class-name>com.vmware.gemfire.rest.JsonFunction</class-name></initializer>\\n&')

            append('{0}/conf/logging.properties'.format(self.instance_name), '\ncom.gemstone.gemfire.modules.session.filter.level = FINE')

            if webapp_jar_mode == 'split':
                run('cp {0}/lib/gemfire.jar {1}/lib'.format(self.artifact_location, self.instance_name))
                run('cp {0}/lib/gemfire-modules-?.?*jar {1}/lib/gemfire-modules.jar'.format(self.artifact_location, self.instance_name))
                run('cp {0}/lib/gemfire-modules-session-?.?*jar {1}/lib/gemfire-modules-session.jar'.format(self.artifact_location, self.instance_name))
                run('cp {0}/lib/slf4j-api*jar {1}/lib/slf4j-api.jar'.format(self.artifact_location, self.instance_name))
                run('cp {0}/lib/slf4j-jdk14*jar {1}/lib/slf4j-jdk14.jar'.format(self.artifact_location, self.instance_name))
                run('cp {0}/lib/{1} {2}/lib'.format(self.artifact_location, env.json_jar_basename, self.instance_name))



    def deploy_webapp(self, host, w, flavor, webapp_jar_mode=None):
        '''
            Deployment consists of running the modify_war script on the remote system
            and then calling Tomcat's manager to deploy the war locally - i.e. from
            the remote system's filesystem.
        '''
        webapp = w.copy()

        # Copy the war file to a remote temp location first
        temp_war = '{0}/{1}'.format(env.scratch_dir, os.path.basename(webapp['war_file']))
        put(webapp['war_file'], temp_war)

        # Adjust modify_war arguments as necessary
        if webapp_jar_mode == 'split':
            webapp['modify_war_args'] += ' -x'
        else:
            webapp['modify_war_args'] += ' -j{0}/lib/{1}'.format(self.artifact_location, env.json_jar_basename)
            # We need a slf4j binding
            slf4j = run('ls {0}/lib/slf4j-jdk14*'.format(self.artifact_location))
            webapp['modify_war_args'] += ' -j{0}'.format(slf4j)

        if flavor == 'gemfire-cs':
            webapp['modify_war_args'] += ' -t client-server -p gemfire.property.cache-xml-file={0}/conf/cache-client.xml'.format(self.artifact_location)
        else:
            webapp['modify_war_args'] += ' -t peer-to-peer -p gemfire.property.cache-xml-file={0}/conf/cache-peer.xml'.format(self.artifact_location)

        webapp['modify_war_args'] += env.modify_war_args

        # Now modify the war with our filter configuration
        new_war = self.modify_war(temp_war, webapp)

        # Call Tomcat manager to deploy a local file
        url = 'http://{0}:{1}/manager/text/deploy'.format(host, self.port)

        query_params = {
            'path' : webapp['context'],
            'update' : 'true',
            'war' : 'file:{0}'.format(new_war)
        }

        try:
            r = requests.get(url,
                    params=query_params,
                    auth=requests.auth.HTTPBasicAuth('admin', 'admin'),
            )

            if r.text.find('FAIL') >= 0:
                error('Deployement failed: {0}'.format(r.text))
            else:
                logging.info('Deployed on %s', host)
                logging.info(r.text)
        except requests.exceptions.ConnectionError, e:
            # Perhaps the webserver is down so we do it manually
            with cd(self.home_dir):
                run('cp {0} {1}/webapps/{2}.war'.format(new_war, self.instance_name, webapp['context']))

        logging.info('Deployed webapp to %s on %s', webapp['context'], host)


Factory.register(TcServerAppserver)
