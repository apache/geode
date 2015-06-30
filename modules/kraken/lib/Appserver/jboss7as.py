import logging
import time
import os.path
import zipfile
import shutil

from .jboss import JBoss
from .factory import Factory

from fabric.api import *
from fabric.context_managers import *
from fabric.contrib.files import *
from fabric.utils import *


class JBoss7AS(JBoss):

    @staticmethod
    def container_type():
        return 'jboss7as'

    def __init__(self, env):
        JBoss.__init__(self, env)
        self.hot_deploy_dir = 'standalone/deployments'


    def cleanup_test_artifact(self):
        JBoss.cleanup_test_artifact(self)
        with cd(self.home_dir):
            run('rm -rf {0}/modules/com/vmware'.format(self.home_dir))

    def customize_instance(self, resource_dir, flavor, webapp_jar_mode):
        JBoss.customize_instance(self, resource_dir, flavor, webapp_jar_mode)

        with cd(self.home_dir):
            module_xml = os.path.join(resource_dir, 'module.xml')

            if webapp_jar_mode == 'split':
                module_dir = 'modules/com/vmware/gemfire/main'
                run('mkdir -p {0}'.format(module_dir))
                run('rm -f {0}/*'.format(module_dir))

                # If any additional jars need to be copied they must also be added
                # to resources/modules.xml
                run('cp {0}/lib/gemfire.jar {1}'.format(self.artifact_location, module_dir))
                run('cp {0}/lib/gemfire-modules-?.?*jar {1}/gemfire-modules.jar'.format(self.artifact_location, module_dir))
                run('cp {0}/lib/gemfire-modules-session-?.?*jar {1}/gemfire-modules-session.jar'.format(self.artifact_location, module_dir))
                run('cp {0}/lib/slf4j-api*jar {1}/slf4j-api.jar'.format(self.artifact_location, module_dir))
                run('cp {0}/lib/slf4j-log4j12*jar {1}/slf4j-log4j12.jar'.format(self.artifact_location, module_dir))
                run('cp {0}/lib/{1} {2}'.format(self.artifact_location, env.json_jar_basename, module_dir))
                put(module_xml, module_dir)


    def start(self):
        stopped = True
        pid_file = '{0}/jboss.pid'.format(env.gemfire_run_dir)
        if exists(pid_file):
            x = run('ps -fp `cat {0}`'.format(pid_file), warn_only=True)
            if x.succeeded:
                stopped = False

        if stopped:
            with cd(self.home_dir), shell_env(JBOSS_PIDFILE=pid_file, LAUNCH_JBOSS_IN_BACKGROUND='1'):
                x = run('dtach -n /tmp/jboss bin/standalone.sh -Drest.json.war=file://{0}/lib/{1} -c standalone.xml -Djboss.bind.address.unsecure=0.0.0.0 -Djboss.bind.address=0.0.0.0 -Djboss.bind.address.management=0.0.0.0 ">standalone/log/console.log" "2>&1"'.format(self.artifact_location, env.json_jar_basename))

        for i in range(10):
            if exists(pid_file):
                p = run('cat {0}'.format(pid_file))
                logging.info('JBoss started with pid {0}'.format(p))
                break
            time.sleep(1)


Factory.register(JBoss7AS)
