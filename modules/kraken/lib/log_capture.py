import sys
import logging

from StringIO import StringIO as sIO

class LoggingStringIO(sIO):
    """
        Class which wraps a logger with a StringIO object. Everything written 
        to the StringIO instance is also logged to the logger passed into the
        constructor. The data logged is logged at new line boundaries.
    """

    def __init__(self, log, level=logging.DEBUG):
        sIO.__init__(self)
        self.log = log
        self.level = level
        self.buffer = ''

    def write(self, stuff):
        sIO.write(self, stuff)
        self.buffer += stuff
        self.log_buffer()

    def close(self):
        sIO.close(self)
        self.log_buffer(True)

    def log_buffer(self, all=False):
        if all:
            self.buffer += '\n'
        n = self.buffer.find('\n')
        while n >= 0:
            line = self.buffer[:n]
            if len(line) > 0:
                self.log.log(self.level, line)
            self.buffer = self.buffer[n+1:]
            n = self.buffer.find('\n')


# This is the decorator - @log_capture.wrap
def wrap(func):
    def callf(*args, **kwargs):
        orig_stdout = sys.stdout
        orig_stderr = sys.stderr
        sys.stdout = LoggingStringIO(logging.getLogger('stdout'))
        sys.stderr = LoggingStringIO(logging.getLogger('stderr'))
        r = ''
        try:
            r = func(*args, **kwargs)
        finally:
            sys.stdout.close()
            sys.stderr.close()
            sys.stdout = orig_stdout
            sys.stderr = orig_stderr
        return r

    return callf


def setup_logging():
    # Set up logging
    p_log = logging.getLogger('paramiko')
    p_log.setLevel(logging.WARNING)

    debug_handler = logging.FileHandler('debug.log')

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    debug_handler.setFormatter(formatter)
    debug_handler.setLevel(logging.DEBUG)
    r_log = logging.getLogger('')
    for h in r_log.handlers:
        h.setLevel(logging.CRITICAL)
    r_log.addHandler(debug_handler)
    r_log.setLevel(logging.DEBUG)


def dump_logging():
    d = logging.Logger.manager.loggerDict
    root = logging.getLogger('')
    logging.info('... root (%s)', root.level)
    for h in root.handlers:
        logging.info('...   %s (%s)', h, h.level)
    for l in d:
        if type(d[l]) != logging.PlaceHolder:
            logging.info('... %s (%s)', l, d[l].level)
            for h in d[l].handlers:
                logging.info('...   %s (%s)', h, h.level)
        else:
            logging.info('... %s (-)', l)


def dump_logging_to_file():
    f = open('logging-debug.log', 'a')
    f.write('----------------------------------------------\n')
    d = logging.Logger.manager.loggerDict
    root = logging.getLogger('')
    f.write('... root (%s)\n' % root.level)
    for h in root.handlers:
        f.write('...   %s (%s)\n' %(h, h.level))
    for l in d:
        if type(d[l]) != logging.PlaceHolder:
            f.write('... %s (%s)\n' % (l, d[l].level))
            for h in d[l].handlers:
                f.write('...   %s (%s)\n' % (h, h.level))
        else:
            f.write('... %s (-)\n' % l)
    f.close()