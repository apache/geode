import Appserver
import os
import inspect
import sys
import logging
import StringIO

from fabric.api import env
from fabric.colors import cyan
from test_functions import *

import log_capture

# Define resources relative to this file
env.resources = os.path.join(os.path.dirname(inspect.getfile(sys._getframe(0))), 'resources')

# Set some defaults
feature_tags = {
    'webapp_jar_mode' : '',
    'setup'           : True,
    'teardown'        : True
}


@log_capture.wrap
def before_feature(context, feature):
    from global_env import ARTIFACTS, APPSERVERS

    context.flavor = feature_tags['flavor']
    context.webapp_jar_mode = feature_tags['webapp_jar_mode']

    context.appserver = Appserver.Factory(feature_tags['appserver'],
            APPSERVERS[feature_tags['appserver']])
    context.artifact_appserver = ARTIFACTS[APPSERVERS[feature_tags['appserver']]['artifact_key']]

    # Mainline setup
    setup()
    if feature_tags['setup']:
        undeploy_webapp(context.appserver, env.webapps['basic'])
        stop_web_server(context.appserver)
        destroy_instance(context.appserver)

        if feature_tags['flavor'] == 'gemfire-cs':
            stop_cacheserver(context.appserver)
            stop_locator(context.appserver)

        deploy_test_artifact(context.appserver, context.artifact_appserver)
        create_instance(context.appserver, context.flavor)
        customize_instance(context.appserver, context.flavor, context.webapp_jar_mode)

        if feature_tags['flavor'] == 'gemfire-cs':
            start_locator(context.appserver)
            start_cacheserver(context.appserver)


@log_capture.wrap
def after_feature(context, feature):
    if not context.failed and feature_tags['teardown']:
        cleanup()
        undeploy_webapp(context.appserver, env.webapps['basic'])
        stop_web_server(context.appserver)

        if feature_tags['flavor'] == 'gemfire-cs':
            stop_cacheserver(context.appserver)
            stop_locator(context.appserver)


@log_capture.wrap
def before_scenario(context, scenario):
    """
        This gets reset by some test methods so we explicitly
        ensure it is set correctly at the start of every scenario.
    """
    env.region_name = 'gemfire_modules_sessions'
    env.modify_war_args = ''


@log_capture.wrap
def after_scenario(context, scenario):
    if not context.failed and hasattr(context, 'webapp_name'):
        undeploy_webapp(context.appserver, env.webapps[context.webapp_name])
        stop_web_server(context.appserver)

        if hasattr(context, 'redeploy_active'):
            restore_redeploy(context.appserver, context.flavor)


def str2bool(v):
    if v.lower() in ("yes", "true", "t", "1"):
        return True
    elif v.lower() in ("no", "false", "n", "0"):
        return False
    else:
        raise RuntimeError()


############################ Mainline ####################################


# Process tags set by environment variables
if 'FEATURE_TAGS' in os.environ:
    for t in os.environ['FEATURE_TAGS'].split(' '):
        n = t.find('=')
        if n >= 0:
            k = t[:n]
            try:
                v = str2bool(t[n+1:])
            except:
                v = t[n+1:]
        else:
            k = t
            v = ''
        feature_tags[k] = v

max_l = max([len(x) for x in feature_tags.keys()])
format_str = '{:>%d} = {}' %  max_l

print cyan('Feature flags:')
feature_keys = feature_tags.keys()
feature_keys.sort()
for k in feature_keys:
    print cyan(format_str.format(k, feature_tags[k]))
print

log_capture.dump_logging()

log_capture.setup_logging()

