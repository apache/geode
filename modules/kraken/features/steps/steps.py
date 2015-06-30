import logging
import log_capture

from test_functions import *
from fabric.api import env

@given('application server is started')
@log_capture.wrap
def step(context):
    start_web_server(context.appserver)


@given('"{webapp_name}" webapp is deployed')
@log_capture.wrap
def step(context, webapp_name):
    deploy_webapp(context.appserver, env.webapps[webapp_name], context.flavor, context.webapp_jar_mode)
    context.webapp_name = webapp_name


@given('application server is set up for redeployment')
@log_capture.wrap
def step(context):
    setup_redeploy(context.appserver, context.flavor)
    # This gets used during after_scenario for cleanup
    context.redeploy_active = True


@then('"{webapp_name}" webapp test url works')
@log_capture.wrap
def step(context, webapp_name):
    test_url(context.appserver, env.webapps[webapp_name])


@then('json contains "{content}" at url path "{url_path}"')
@log_capture.wrap
def step(context, content, url_path):
    json_contains(context.appserver, url_path, content)


@then('json should not contain "{content}" at url path "{url_path}"')
@log_capture.wrap
def step(context, content, url_path):
    exception = False
    try:
        json_contains(context.appserver, url_path, content)
    except:
        exception = True
    if not exception: 
        raise RuntimeError("JSON should not contain '{0}' but it does".format(content))


@then('basic caching works')
@log_capture.wrap
def step(context):
    basic_caching_1(context.appserver, env.webapps[context.webapp_name])


@then('basic cache failover works')
@log_capture.wrap
def step(context):
    basic_failover_1(context.appserver, env.webapps[context.webapp_name])


@then('webapp reloading works')
@log_capture.wrap
def step(context):
    webapp_reload_1(context.appserver,
            env.webapps[context.webapp_name],
            context.flavor,
            context.webapp_jar_mode)


@then('webapp reloading works with active sessions')
@log_capture.wrap
def step(context):
    webapp_reload_2(context.appserver,
            env.webapps[context.webapp_name],
            context.flavor,
            context.webapp_jar_mode)


@then('restore customized region')
@log_capture.wrap
def step(context):
    if context.flavor == 'gemfire-cs':
        stop_cacheserver(context.appserver)

    restore_customized_region(context.appserver, context.flavor)

    if context.flavor == 'gemfire-cs':
        start_locator(context.appserver)
        start_cacheserver(context.appserver)


@given('cache server is restarted')
@log_capture.wrap
def step(context):
    if context.flavor == 'gemfire-cs':
        stop_cacheserver(context.appserver)
        start_cacheserver(context.appserver)


@given('gemfire configuration is using region path "{region_path}" and xml template id "{template_id}"')
@log_capture.wrap
def step(context, region_path, template_id):
    if context.flavor == 'gemfire-cs':
        stop_cacheserver(context.appserver)

    customize_cache_xml(context.appserver, context.flavor, region_path, template_id)

    if context.flavor == 'gemfire-cs':
        start_locator(context.appserver)
        start_cacheserver(context.appserver)

    env.modify_war_args += ' -p gemfire.cache.region_name={0}'.format(region_path)


@given('debug cache listener is enabled')
@log_capture.wrap
def step(context):
    env.modify_war_args += ' -p gemfire.cache.enable_debug_listener=true'


@given('client is configured for locator using region path "{region_path}" and xml template id "{template_id}"')
@log_capture.wrap
def step(context, region_path, template_id):
    if context.flavor == 'gemfire-cs':
        customize_cache_client_xml(context.appserver, 'locator')


@given('client cache is configured for "{cache_type}"')
@log_capture.wrap
def step(context, cache_type):
    if context.flavor == 'gemfire-cs':
        context.appserver.disable_local_cache()


@then('non-sticky sessions work')
@log_capture.wrap
def step(context):
    test_non_sticky_sessions(context.appserver, env.webapps[context.webapp_name])


@then('setting maxInactiveInterval to "{200}" works')
@log_capture.wrap
def step(context, interval):
    test_set_max_inactive_interval(context.appserver, env.webapps[context.webapp_name], interval)
