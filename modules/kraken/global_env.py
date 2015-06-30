import os.path
from fabric.api import *

ARTIFACTS = {
    'tcserver'  : '/Users/jdeppe/git/gemfireModules/gemfire-modules-assembly/target/vFabric_GemFire_Modules-7.0.1-tcServer.zip',
    'appserver' : '/Users/jdeppe/git/gemfireModules/gemfire-modules-assembly/target/vFabric_GemFire_Modules-7.0.1-AppServer.zip'
}

APPSERVERS = {
    'tcserver_native': {
        'home_dir'      : '/home/jdeppe/tcServer/vfabric-tc-server-developer-2.8.0.RELEASE',
        'instance_name' : 'module-test',
        'port'          : '8080',
        'java_home'     : '/usr/lib/jvm/java-6-sun',
        'artifact_key'  : 'tcserver'
    },
    'tcserver_appserver': {
        'home_dir'      : '/home/jdeppe/tcServer/vfabric-tc-server-developer-2.8.0.RELEASE',
        'instance_name' : 'module-test',
        'port'          : '8080',
        'java_home'     : '/usr/lib/jvm/java-6-sun',
        'artifact_key'  : 'appserver'
    },
    'jboss7as': {
        'home_dir'      : '/home/jdeppe/jboss/jboss-as-7.1.1.Final',
        'instance_name' : 'gemfire-modules',
        'port'          : '8080',
        'java_home'     : '/usr/lib/jvm/java-6-sun',
        'artifact_key'  : 'appserver'
    },
    'jboss6': {
        'home_dir'      : '/home/jdeppe/jboss/jboss-6.1.0.Final',
        'instance_name' : 'gemfire-modules',
        'port'          : '8080',
        'java_home'     : '/usr/lib/jvm/java-6-sun',
        'artifact_key'  : 'appserver'
    }
}

env.roledefs = {
    'load_balancer'  : [],
    'web_server'     : ['websvr-vm-1.gemstone.com', 'websvr-vm-2.gemstone.com'],
    'gemfire_server' : ['websvr-vm-1.gemstone.com', 'websvr-vm-2.gemstone.com'],
    'gemfire_locator': ['websvr-vm-1.gemstone.com']
}

# Using fabric's env as a global location for GemFire variables. These settings
# are visible to other code running fabric commands.
env.gemfire_home = '/home/jdeppe/gemfire'
env.gemfire_run_dir = '/tmp/module-tests-gemfire'
env.scratch_dir = '/tmp/module-tests-{0}'.format(os.getpid())
env.locator_port = '19991'
env.json_port = '5757'
env.json_jar = '/Users/jdeppe/projects/reset-test/target/rest-json-function.jar'
env.json_jar_basename = os.path.basename(env.json_jar)

env.webapps = {
    'basic': {
        'context'        : '/test',
        'url'            : '/test/ReplicationServlet',
        'war_file'       : '/Users/jdeppe/projects/test-web-sessions/target/test-web-sessions.war',
        'modify_war_args': '-J-Dhttp.proxyHost=proxy.eng.vmware.com -J-Dhttp.proxyPort=3128'
    },
    'basic2': {
        'context'        : '/test2',
        'url'            : '/test2/ReplicationServlet',
        'war_file'       : '/Users/jdeppe/projects/test-web-sessions/target/test-web-sessions.war',
        'modify_war_args': '-J-Dhttp.proxyHost=proxy.eng.vmware.com -J-Dhttp.proxyPort=3128'
    },
    'basic-filter-split': {
        'context'        : '/test',
        'url'            : '/test/ReplicationServlet',
        'war_file'       : '/Users/jdeppe/projects/test-web-sessions/target/test-web-sessions.war',
        'modify_war_args': '-J-Dhttp.proxyHost=proxy.eng.vmware.com -J-Dhttp.proxyPort=3128'
    }
}

########################  Env settings for fabric  #########################

env.abort_on_prompts = True
# Must have this otherwise locators, cacheservers and tcServer fail to start
env.always_use_pty = False

#env.parallel = True
