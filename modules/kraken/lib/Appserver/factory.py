
class Factory(object):

    containers = {}

    def __new__(klass, c_type, env={}):
        if Factory.containers.has_key(c_type):
            return Factory.containers[c_type](env)
        else:
            raise TypeError("Unknown container type: {0}".format(c_type))

    @staticmethod
    def register(klass):
        Factory.containers[klass.container_type()] = klass
