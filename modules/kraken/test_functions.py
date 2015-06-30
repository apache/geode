import Appserver
import time
import os
import os.path
import errno
import shutil
import requests
import logging
import zipfile
import xml.etree.ElementTree as ET

from fabric.api import *
from fabric.tasks import *
from fabric.network import *
from fabric.contrib.files import *

# decorator implementation
def execute_keyword(func):
    def callf(*args, **kwargs):
        return execute(func, *args, **kwargs)
    return callf


########################  Task definitions used by fabric  #########################

def setup():
    local_setup()
    remote_setup()

def local_setup():
    # Make our local scratch dir if it doesn't already exist
    try:
        os.makedirs(env.scratch_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(env.scratch_dir):
            pass
        else:
            raise

@execute_keyword
@task
#@parallel
@roles('web_server')
def remote_setup():
    run('mkdir -p {0}'.format(env.scratch_dir))
    run('mkdir -p {0}'.format(env.gemfire_run_dir))

    # Make our local scratch dir if it doesn't already exist
    try:
        os.makedirs(env.scratch_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(env.scratch_dir):
            pass
        else:
            raise


def cleanup():
    remote_cleanup()
    local_cleanup()


def local_cleanup():
    try:
        shutil.rmtree(env.scratch_dir)
    except OSError as exc:
        raise


@execute_keyword
@task
#@parallel
@roles('web_server')
def remote_cleanup():
    run('rm -rf {0}'.format(env.scratch_dir))


@execute_keyword
@task
#@parallel
@roles('web_server')
def deploy_test_artifact(svr, artifact):
    f = put(artifact, '/tmp')
    svr.deploy_test_artifact(f[0])


@execute_keyword
@task
#@parallel
@roles('web_server')
def cleanup_test_artifact(svr):
    svr.cleanup_test_artifact()


@execute_keyword
@task
#@parallel
@roles('web_server')
def create_instance(svr, template=None, force=False):
    with cd(svr.home_dir):
        if force or not exists(svr.instance_name):
            svr.create_instance(env.gemfire_home, template)


@execute_keyword
@task
@roles('web_server')
def destroy_instance(svr):
    svr.destroy_instance()


@execute_keyword
@task
@roles('web_server')
def cleanup_templates(svr):
    svr.cleanup_templates()


@execute_keyword
@task
@roles('web_server')
def customize_instance(svr, flavor, webapp_jar_mode=None):
    svr.customize_instance(env.resources, flavor, webapp_jar_mode)


@execute_keyword
@task
@roles('web_server')
def customize_cache_xml(svr, flavor, region_name, template_id):
    svr.customize_cache_xml(flavor, region_name, template_id)
    env.region_name = region_name


@execute_keyword
@task
@roles('web_server')
def customize_cache_client_xml(svr, template_id):
    src = 'cache-client.{0}.xml'.format(template_id)
    dst = '{0}/conf/cache-client.xml'.format(svr.instance_name)

    with cd(svr.home_dir):
        upload_template(src, dst,
                {'host': env.roledefs['gemfire_locator'][0], 'port': env.locator_port},
                use_jinja=True, template_dir=env.resources)


@execute_keyword
@task
@roles('web_server')
def restore_customized_region(svr, flavor):
    svr.restore_customized_region(flavor)


@execute_keyword
@task
@roles('web_server')
def setup_redeploy(svr, flavor):
    svr.setup_redeploy(flavor)


@execute_keyword
@task
@roles('web_server')
def restore_redeploy(svr, flavor):
    svr.restore_redeploy(flavor)


@execute_keyword
@task
@roles('gemfire_locator')
def start_locator(svr):
    x = run('{0}/bin/gemfire.sh status-locator -dir={1}'.format(svr.gemfire_modules_home, env.gemfire_run_dir))
    if x != 'running':
        run('{0}/bin/gemfire.sh start-locator -dir={1} -port={2}'.format(svr.gemfire_modules_home, env.gemfire_run_dir, env.locator_port))
    env.locator_str = '{0}[{1}]'.format(env.host, env.locator_port)


@execute_keyword
@task
@roles('gemfire_locator')
def stop_locator(svr):
    run('{0}/bin/gemfire.sh stop-locator -dir={1} -port={2}'.format(svr.gemfire_modules_home, env.gemfire_run_dir, env.locator_port), warn_only=True, quiet=True)


@execute_keyword
@task
@roles('gemfire_server')
def start_cacheserver(svr):
    x = run('{0}/bin/cacheserver.sh status -dir={1}'.format(svr.gemfire_modules_home, env.gemfire_run_dir))
    if x.find('running') < 0:
        run('{0}/bin/cacheserver.sh start -classpath={0}/lib/{1} -dir={2} locators={3} cache-xml-file={0}/conf/cache-server.xml'.format(
                svr.gemfire_modules_home,
                env.json_jar_basename,
                env.gemfire_run_dir,
                env.locator_str))


@execute_keyword
@task
@roles('gemfire_server')
def stop_cacheserver(svr):
    run('{0}/bin/cacheserver.sh stop -dir={1}'.format(svr.gemfire_modules_home, env.gemfire_run_dir), warn_only=True, quiet=True)


@execute_keyword
@task
@roles('web_server')
def start_web_server(svr):
    svr.start()
    _test_url(svr, "/", 30)


@execute_keyword
@task
@roles('web_server')
def stop_web_server(svr):
    svr.stop()


@execute_keyword
@task
@serial
@roles('web_server')
def deploy_webapp(svr, w, flavor, webapp_jar_mode=None):
    webapp = w.copy()
    if flavor == 'gemfire-p2p':
        add_json_function_listener(webapp)
    svr.deploy_webapp(env.host, webapp, flavor, webapp_jar_mode)
    _test_url(svr, webapp['url'], 30)


@execute_keyword
@task
@roles('web_server')
def undeploy_webapp(svr, webapp):
    svr.undeploy_webapp(env.host, webapp['context'])


@execute_keyword
@task
@roles('web_server')
def test_url(svr, webapp, tries=1):
    _test_url(svr, webapp['url'], tries)

def _test_url(svr, path, tries=1):
    url = 'http://{0}:{1}{2}'.format(env.host, svr.port, path)
    r = None
    for i in range(tries):
        try:
            r = requests.get(url)
            if r.status_code == 200:
                break
        except requests.exceptions.ConnectionError, e:
            pass

        if i < tries - 1:
            time.sleep(2)

    if not r:
        raise RuntimeError('URL {0} unable to connect'.format(url))

    if r.status_code != 200:
        raise RuntimeError('URL {0} responded with status: {1}'.format(url, r.status_code))
    else:
        logging.info('%s responded with status %s', url, r.status_code)


@execute_keyword
@task
@roles('web_server')
def json_contains(svr, path, content=None):
    url = 'http://{0}:{1}{2}'.format(env.host, env.json_port, path)
    r = requests.get(url)
    if r.status_code != 200:
        print r.text
        raise RuntimeError('URL {0} responded with status: {1}'.format(url, r.status_code))
    else:
        if content:
            if r.text.find(content) >= 0:
                logging.info('%s responded with status %s', url, r.status_code)
            else:
                print r.text
                raise RuntimeError('URL {0} did not contain expected content: {1}'.format(url, content))


@execute_keyword
@task
def basic_caching_1(svr, webapp):
    # Attributes to create
    attr_count = 10

    hosts = env.roledefs['web_server']
    url = 'http://{0}:{1}{2}'.format(hosts[0], svr.port, webapp['url'])

    session_id = create_attributes(url, attr_count)
    cookies = {'JSESSIONID' : session_id}

    # Check if our session is visible in the caches
    for h in hosts:
        json_url = 'http://{0}:{1}/json/regions/{2}/key/{3}'.format(
                h, env.json_port, env.region_name, session_id)
        r = requests.get(json_url)
        if r.status_code != 200:
            raise RuntimeError('URL {0} responded with status: {1}'.format(json_url, r.status_code))
        j = r.json()
        if len(j['valueNames']) != 10:
            print r.text
            raise RuntimeError('URL {0} does not contain {1} attributes, only {2}'.format(json_url, attr_count, len(j['valueNames'])))


    # Check if we get a response from the other web servers using the same session id
    for h in hosts[1:]:
        url = 'http://{0}:{1}{2}'.format(h, svr.port, webapp['url'])
        r = requests.get(url, cookies=cookies)
        if r.status_code != 200:
            print r.text
            raise RuntimeError('URL {0} responded with status: {1}'.format(url, r.status_code))
        if not r.text.find('Number of attributes in session: {0}'.format(attr_count)) >= 0:
            print r.text
            raise RuntimeError('URL {0} does not contain {1} attributes'.format(url, attr_count))


@execute_keyword
@task
def basic_failover_1(svr, webapp):
    # Attributes to create
    attr_count = 10

    hosts = env.roledefs['web_server']
    url = 'http://{0}:{1}{2}'.format(hosts[0], svr.port, webapp['url'])

    session_id = create_attributes(url, attr_count)
    cookies = {'JSESSIONID' : session_id}

    env.host_string = hosts[0]
    svr.kill()

    # Check if our session is visible in the caches
    for h in hosts[1:]:
        json_url = 'http://{0}:{1}/json/regions/{2}/key/{3}'.format(
                h, env.json_port, env.region_name, session_id)
        r = requests.get(json_url)
        if r.status_code != 200:
            raise RuntimeError('URL {0} responded with status: {1}'.format(json_url, r.status_code))
        j = r.json()
        if len(j['valueNames']) != 10:
            print r.text
            raise RuntimeError('URL {0} does not contain {1} attributes, only {2}'.format(
                    json_url, attr_count, len(j['valueNames'])))


    # Check if we get a response from the other web servers using the same session id
    for h in hosts[1:]:
        url = 'http://{0}:{1}{2}'.format(h, svr.port, webapp['url'])
        r = requests.get(url, cookies=cookies)
        if r.status_code != 200:
            print r.text
            raise RuntimeError('URL {0} responded with status: {1}'.format(url, r.status_code))
        if not r.text.find('Number of attributes in session: {0}'.format(attr_count)) >= 0:
            print r.text
            raise RuntimeError('URL {0} does not contain {1} attributes'.format(url, attr_count))


@execute_keyword
@task
def webapp_reload_1(svr, webapp, flavor, webapp_jar_mode=None, client_proxy=None):
    """
        Very basic reload test that ignores some basic issues - just testing sanity
    """
    # Attributes to create
    attr_count = 10

    hosts = env.roledefs['web_server']
    url = 'http://{0}:{1}{2}'.format(hosts[0], svr.port, webapp['url'])

    session_id = create_attributes(url, attr_count)

    # Force a reload - this will automagically happen on all web servers
    deploy_webapp(svr, webapp, flavor, webapp_jar_mode)
    #svr.deploy_webapp(hosts[1], webapp['context'], webapp['war_file'])

    if client_proxy:
        # Add an attribute to the session
        url = 'http://{0}:{1}{2}'.format(hosts[0], svr.port, webapp['url'])
        add_attributes(url, session_id, attr_count, 1)
        attr_count += 1

    # Check if our session is visible in the caches
    json_session_visible(hosts, session_id, attr_count)

    # Check if the session is still OK
    url = 'http://{0}:{1}{2}'.format('{0}', svr.port, webapp['url'])
    test_str = 'Number of attributes in session: {0}'.format(attr_count)
    http_session_visible(hosts, session_id, url, test_str)


@execute_keyword
@task
def webapp_reload_2(svr, webapp, flavor, webapp_jar_mode=None):
    """
        More complex scenario. Here we're unloading on one server and expecting
        the other server to still have the data.

        This test is dependant on the clocks, between the servers being
        accurate within 5 seconds of each other, otherwise results may be
        inaccurate.
    """
    # Attributes to create
    attr_count = 10

    hosts = env.roledefs['web_server']
    url = 'http://{0}:{1}{2}'.format(hosts[0], svr.port, webapp['url'])

    session_id = create_attributes(url, attr_count)

    # Undeploy on one server
    svr.undeploy_webapp(hosts[0], webapp['context'])
    logging.info('### Undeployed webapp on %s', hosts[0])

    # Check if our session is visible in the other caches
    json_session_visible(hosts[1:], session_id, attr_count)

    # Check if the session is still OK
    url = 'http://{0}:{1}{2}'.format('{0}', svr.port, webapp['url'])
    test_str = 'Number of attributes in session: {0}'.format(attr_count)
    http_session_visible(hosts[1:], session_id, url, test_str)

    # Now sleep to cater for clock discrepancies
    time.sleep(4)

    # Add an attribute to the session
    url = 'http://{0}:{1}{2}'.format(hosts[1], svr.port, webapp['url'])
    add_attributes(url, session_id, attr_count, 2)
    attr_count += 2

    # Check webapp again
    url = 'http://{0}:{1}{2}'.format('{0}', svr.port, webapp['url'])
    test_str = 'Number of attributes in session: {0}'.format(attr_count)
    http_session_visible(hosts[1:], session_id, url, test_str)

    # Check json again
    # If we're doing client-server then the json port is in the cache server 
    # and not tied to the (down) webapp.
    if flavor == 'gemfire-cs':
        json_session_visible(hosts, session_id, attr_count)
    else:
        json_session_visible(hosts[1:], session_id, attr_count)

    # Redeploy on first server
    svr.deploy_webapp(hosts[0], webapp, flavor, webapp_jar_mode)
    logging.info('### Redeployed webapp on %s', hosts[0])

    # Check if our session is visible in ALL the other caches
    json_session_visible(hosts, session_id, attr_count)

    # Check if the session is still OK on ALL servers
    url = 'http://{0}:{1}{2}'.format('{0}', svr.port, webapp['url'])
    test_str = 'Number of attributes in session: {0}'.format(attr_count)
    http_session_visible(hosts, session_id, url, test_str)


@execute_keyword
@task
def test_non_sticky_sessions(svr, webapp):
    attr_count = 1
    hosts = env.roledefs['web_server']
    url = 'http://{0}:{1}{2}'.format(hosts[0], svr.port, webapp['url'])
    session_id = create_attributes(url, 1)

    for i in range(1, 10):
        url = 'http://{0}:{1}{2}'.format(hosts[attr_count % len(hosts)], svr.port, webapp['url'])
        add_attributes(url, session_id, attr_count, 1)
        attr_count += 1

    # Check if our session is visible in the caches
    json_session_visible(hosts, session_id, attr_count)

    # Check if the session is still OK
    url = 'http://{0}:{1}{2}'.format('{0}', svr.port, webapp['url'])
    test_str = 'Number of attributes in session: {0}'.format(attr_count)
    http_session_visible(hosts, session_id, url, test_str)


@execute_keyword
@task
def test_set_max_inactive_interval(svr, webapp, interval):
    attr_count = 1
    hosts = env.roledefs['web_server']
    url = 'http://{0}:{1}{2}'.format(hosts[0], svr.port, webapp['url'])
    session_id = create_attributes(url, 3)

    cookies = {'JSESSIONID' : session_id}

    # set new maxInactiveInterval setting
    payload = {
        'showSession' : 'true',
        'validateSessionContents' : 'true',
        'maxInactiveInterval' : interval,
        'command' : 'updateMaxInactiveInterval'
    }

    r = requests.post(url, data=payload, cookies=cookies)
    if r.status_code != 200:
        print r.text
        raise RuntimeError('url {0} responded with status: {1}'.format(url, r.status_code))

#    for h in hosts:
#        j = get_json(h, session_id)
#        print j

    logging.info('maxInactiveInterval updated to %s', interval)

    # Check if the session is still OK
    url = 'http://{0}:{1}{2}'.format('{0}', svr.port, webapp['url'])
    test_str = 'session max inactive interval: {0}'.format(interval)
    http_session_visible(hosts[:1], session_id, url, test_str)

#    for h in hosts:
#        j = get_json(h, session_id)
#        print j

    http_session_visible(hosts[1:], session_id, url, test_str)

#######################################################################
###  Internal helper functions

def add_json_function_listener(webapp):
    # Copy the war file in preparation of modifying it
    new_file = os.path.join(env.scratch_dir, os.path.basename(webapp['war_file']))
    shutil.copyfile(webapp['war_file'], new_file)
    webapp['war_file'] = new_file

    # Using with causes the opened file to automatically be closed
    with zipfile.ZipFile(new_file, 'a') as war:
        # Register default namespace otherwise we get 'ns0' prepended on every element
        ET.register_namespace('', 'http://java.sun.com/xml/ns/j2ee')
        web_xml = ET.fromstring(war.read('WEB-INF/web.xml'))
        listener = ET.Element('listener')
        ET.SubElement(listener, 'listener-class').text = 'com.vmware.gemfire.rest.JsonFunction'
        web_xml.insert(0, listener)
        war.writestr('WEB-INF/web.xml', ET.tostring(web_xml))


def get_json(host, session_id):
    json_url = 'http://{0}:{1}/json/regions/{2}/key/{3}'.format(
            host, env.json_port, env.region_name, session_id)
    r = requests.get(json_url)
    if r.status_code != 200:
        print r.text
        raise RuntimeError('URL {0} responded with status: {1}'.format(
                json_url, r.status_code))
    return r.json()


def json_session_visible(hosts, session_id, attr_count=None):
    """
        Check if our session is visible in the caches. Throw exception if not.
    """
    for h in hosts:
        j = get_json(h, session_id)
        if attr_count and len(j['valueNames']) != attr_count:
            logging.info(j)
            raise RuntimeError('JSON request for session {0} on {1} does not contain {2} attributes, only {3}'.format(
                    session_id, h, attr_count, len(j['valueNames'])))
        #logging.info(j)
        logging.info('{0} creationTime {1}'.format(h, j['creationTime']))
        logging.info('{0} lastAccessedTime {1}'.format(h, j['lastAccessedTime']))


def http_session_visible(hosts, session_id, url, test_str=None):
    """
        Check if we can get a page containing a particular string.
        The url passed in is just a template with a placeholder for the host.
    """
    cookies = {'JSESSIONID' : session_id}
    for h in hosts:
        u = url.format(h)
        r = requests.get(u, cookies=cookies)
        if r.status_code != 200:
            print r.text
            raise RuntimeError('URL {0} responded with status: {1} [{2}]'.format(
                    u, r.status_code, session_id))
        if test_str and not r.text.find(test_str) >= 0:
            print r.text
            raise RuntimeError('URL {0} with session id {1} does not contain desired test string "{2}"'.format(
                    u, session_id, test_str))



def create_attributes(url, count):
    """ 
        Create a number of attributes and return the session id.
    """
    r = requests.get(url)

    session_id = r.cookies['JSESSIONID']
    cookies = {'JSESSIONID' : session_id}

    # create new attributes in our session which should be visible on the other servers
    for i in range(count):
        payload = {
            'showSession' : 'true',
            'validateSessionContents' : 'true',
            'attr' : 'attrName' + str(i),
            'stringSize' : '1',
            'mapSize' : '1',
            'command' : 'add'
        }

        r = requests.post(url, data=payload, cookies=cookies)
        if r.status_code != 200:
            print r.text
            raise RuntimeError('url {0} responded with status: {1}'.format(url, r.status_code))

    logging.info('%s attributes created for session %s', count, session_id)
    return session_id


def add_attributes(url, session_id, start, count):
    """ 
        Add a number of attributes to an existing session
    """
    cookies = {'JSESSIONID' : session_id}

    # create new attributes in our session
    for i in range(start, start + count):
        payload = {
            'showSession' : 'true',
            'validateSessionContents' : 'true',
            'attr' : 'attrName' + str(i),
            'stringSize' : '1',
            'mapSize' : '1',
            'command' : 'add'
        }

        r = requests.post(url, data=payload, cookies=cookies)
        if r.status_code != 200:
            print r.text
            raise RuntimeError('url {0} responded with status: {1}'.format(url, r.status_code))

    logging.info('%s attribute(s) added to existing session %s', count, session_id)
    return session_id


if __name__ == '__main__':
    import global_env

    env.scratch_dir = '/tmp/module-tests-{0}'.format(os.getpid())

    add_json_function_listener(env.webapps['basic'])
