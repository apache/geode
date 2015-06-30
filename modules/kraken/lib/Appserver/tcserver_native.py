import requests 
import os
import logging

from .factory import Factory
from .tcserver import TcServer
from .abstract_appserver import AbstractAppserver

from fabric.api import *
from fabric.context_managers import *
from fabric.contrib.files import *
from fabric.utils import *

class TcServerNative(TcServer):

    @staticmethod
    def container_type():
        return "tcserver_native"

    def __init__(self, env):
        AbstractAppserver.__init__(self, env)
        self.artifact_location = '{0}/templates'.format(self.home_dir)
        self.gemfire_modules_home = '{0}/{1}'.format(self.home_dir, self.instance_name)

    def deploy_test_artifact(self, artifact):
        self.cleanup_test_artifact()
        with cd(self.home_dir):
            run('unzip -o -d templates {0}'.format(artifact))

    def cleanup_test_artifact(self):
        with cd(self.home_dir):
            run('rm -rf {0}/gemfire-*'.format(self.artifact_location))

    def create_instance(self, gemfire_home, template):
        with cd(self.home_dir), shell_env(JAVA_HOME=self.java_home):
            run('rm -rf {0}'.format(self.instance_name))
            run('./tcruntime-instance.sh create {0} -t {1}'.format(self.instance_name, template))


    def customize_instance(self, resource_dir, flavor, webapp_jar_mode):
        tomcat_users = os.path.join(resource_dir, 'tomcat-users.xml')
        with cd(self.home_dir):
            run('cp {0}/lib/gemfire.jar {1}/lib/'.format(env.gemfire_home, self.instance_name))
            run('cp {0}/lib/antlr.jar {1}/lib/'.format(env.gemfire_home, self.instance_name))
            put(env.json_jar, '{0}/lib/'.format(self.instance_name))
            put(tomcat_users, '{0}/conf/tomcat-users.xml'.format(self.instance_name))

            if flavor == 'gemfire-cs':
                cache_xml = '{0}/conf/cache-server.xml'.format(self.instance_name)
            else:
                cache_xml = '{0}/conf/cache-peer.xml'.format(self.instance_name)

            if not contains(cache_xml, 'JsonFunction'):
                sed(cache_xml, '</cache>',
                        '<initializer><class-name>com.vmware.gemfire.rest.JsonFunction</class-name></initializer>\\n&')

            append('{0}/conf/logging.properties'.format(self.instance_name), '\ncom.gemstone.gemfire.modules.session.catalina.level = FINE')


    def customize_cache_xml(self, flavor, region_name, template_id):
        if flavor == 'gemfire-p2p':
            src = 'cache-peer.{0}.xml'.format(template_id)
            dst = '{0}/conf/cache-peer.xml'.format(self.instance_name)
        else:
            src = 'cache-server.{0}.xml'.format(template_id)
            dst = '{0}/conf/cache-server.xml'.format(self.instance_name)

        with cd(self.home_dir):
            upload_template(src, dst, {'region_name': region_name},
                    use_jinja=True, template_dir=env.resources)

        catalina_props = '{0}/conf/catalina.properties'.format(self.instance_name)
        with cd(self.home_dir):
            sed(catalina_props, 'region.name=.*', 'region.name={0}'.format(region_name))


    def restore_customized_region(self, flavor):
        if flavor == 'gemfire-p2p':
            src = '{0}/conf/cache-peer.xml.bak'.format(self.instance_name)
            dst = '{0}/conf/cache-peer.xml'.format(self.instance_name)
        else:
            src = '{0}/conf/cache-client.xml.bak'.format(self.instance_name)
            with cd(self.home_dir):
                if exists(src):
                    dst = '{0}/conf/cache-client.xml'.format(self.instance_name)
                    run('mv {0} {1}'.format(src, dst))

            src = '{0}/conf/cache-server.xml.bak'.format(self.instance_name)
            dst = '{0}/conf/cache-server.xml'.format(self.instance_name)

        with cd(self.home_dir):
            run('mv {0} {1}'.format(src, dst))

        catalina_props = '{0}/conf/catalina.properties'.format(self.instance_name)
        with cd(self.home_dir):
            sed(catalina_props, 'region.name=.*', 'region.name=gemfire_modules_sessions')


    def deploy_webapp(self, host, webapp, flavor, webapp_jar_mode=None):
        '''
            Call Tomcat's manager to deploy the war file remotely - i.e. push
            the war from the local system running the test.
        '''
        url = 'http://{0}:{1}/manager/text/deploy'.format(host, self.port)
        war_data = open('/Users/jdeppe/projects/test-web-sessions/target/test-web-sessions.war').read()

        query_params = {
            'path' : webapp['context'],
            'update' : 'true',
        }

        try:
            r = requests.put(url,
                    params=query_params,
                    auth=requests.auth.HTTPBasicAuth('admin', 'admin'),
                    data=war_data
            )

            if r.text.find('FAIL') >= 0:
                error('Deployement failed: {0}'.format(r.text))
            else:
                logging.info('Deployed on %s', host)
                logging.info(r.text)
        except requests.exceptions.ConnectionError, e:
            # Perhaps the webserver is down so we do it manually
            run('rm -rf {0}/{1}/webapps/{2}'.format(
                    self.home_dir, self.instance_name, webapp['context']))
            run('rm -f {0}/{1}/webapps/{2}.war'.format(
                    self.home_dir, self.instance_name, webapp['context']))
            put(webapp['war_file'], '{0}/{1}/webapps/{2}.war'.format(
                    self.home_dir, self.instance_name, webapp['context']))


    def disable_local_cache(self):
        catalina_props = '{0}/conf/catalina.properties'.format(self.instance_name)
        with cd(self.home_dir):
            sed(catalina_props, 'enable.local.cache=.*', 'enable.local.cache=false')



Factory.register(TcServerNative)
