import os.path
import re

from fabric.api import *
from fabric.contrib.files import *

class AbstractAppserver(object):

    base_settings = [
        "home_dir",
        "instance_name",
        "java_home",
        "port"
    ]

    def __init__(self, env={}):
        self.env = env
        self.env.setdefault("port", "8080")
        self.validate()
        self.artifact_location = '{0}/gemfire-modules'.format(self.home_dir)
        self.gemfire_modules_home = self.artifact_location

    def __getattr__(self, attrname):
        return self.env[attrname]


    def container_type(self):
        raise NotImplementedError()


    def start(self):
        raise NotImplementedError()


    def stop(self):
        raise NotImplementedError()


    def validate(self):
        """
            Perform various checks to ensure that we have all the right
            settings.
        """
        for i in AbstractAppserver.base_settings:
            if not self.env.has_key(i):
                raise LookupError("Required env param not found: {0}".format(i))


    def modify_war(self, war_file, webapp):
        """
            Modify war file in order to integrate session replication
        """

        new_file = '{0}/sessions-{1}'.format(env.scratch_dir, os.path.basename(war_file))
        script_args = self.__resolve_params(webapp['modify_war_args'])
        run('{0}/bin/modify_war -v -w {1} -o {2} {3}'.format(
            self.artifact_location,
            war_file,
            new_file,
            script_args
        ))
        
        return new_file


    def setup_redeploy(self, modifiers):
        pass


    def restore_redeploy(self, modifiers):
        pass


    def create_instance(self, flavor):
        pass


    def destroy_instance(self):
        pass


    def customize_cache_xml(self, flavor, region_name, template_id):
        if flavor == 'gemfire-p2p':
            src = 'cache-peer.{0}.xml'.format(template_id)
            dst = '{0}/conf/cache-peer.xml'.format(self.artifact_location)
        else:
            src = 'cache-server.{0}.xml'.format(template_id)
            dst = '{0}/conf/cache-server.xml'.format(self.artifact_location)

        with cd(self.home_dir):
            upload_template(src, dst, {'region_name': region_name},
                    use_jinja=True, template_dir=env.resources)


    def restore_customized_region(self, flavor):
        if flavor == 'gemfire-p2p':
            src = '{0}/conf/cache-peer.xml.bak'.format(self.artifact_location)
            dst = '{0}/conf/cache-peer.xml'.format(self.artifact_location)
        else:
            src = '{0}/conf/cache-client.xml.bak'.format(self.artifact_location)
            with cd(self.home_dir):
                if exists(src):
                    dst = '{0}/conf/cache-client.xml'.format(self.artifact_location)
                    run('mv {0} {1}'.format(src, dst))

            src = '{0}/conf/cache-server.xml.bak'.format(self.artifact_location)
            dst = '{0}/conf/cache-server.xml'.format(self.artifact_location)

        with cd(self.home_dir):
            if exists(src):
                run('mv {0} {1}'.format(src, dst))


    def disable_local_cache(self):
        env.modify_war_args += ' -p gemfire.cache.enable_local_cache=false'


    def deploy_test_artifact(self, artifact):
        self.cleanup_test_artifact()
        with cd(self.home_dir):
            run('unzip -o -d {0} {1}'.format(self.artifact_location, artifact))


    def cleanup_test_artifact(self):
        with cd(self.home_dir):
            run('rm -rf {0}'.format(self.artifact_location))


    def __replace_fn(self, match_obj):
        try:
            r = eval(match_obj.group(1))
        except NameError:
            r = match_obj.group(0)
        except AttributeError:
            r = match_obj.group(0)
        return r


    def __resolve_params(self, param_str):
        """
            Strings can be parameterized with actual code which will be
            evaluated and substituted into the string.
        """
        return re.sub('\{([^\}]*)\}', self.__replace_fn, param_str)
