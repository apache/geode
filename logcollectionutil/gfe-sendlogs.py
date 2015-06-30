#!/usr/bin/env python
import commands
import ftplib
import time
import getopt
import sys
import os
import zipfile

__version__ = .01

DEBUG = False
TIMESTAMP = time.strftime("%m-%d-%Y-%H%M%S")
# Default options
info_container = {"locator": "localhost",
                  "port": "10334",
                  "ftp-server": "ftp.gemstone.com"
                  }


def usage():
    """
    Print usage and exit.
    """
    print "Usage: %s -c <company> -o <output-dir> [OPTION]\n" % os.path.basename(sys.argv[0])
    print "Required arguments"
    print "\t-c, --company=COMPANY\t\tcompany name "
    print "\t-o, --output-dir=DIR\t\tdirectory to stage logs"

    print "\nOptional arguments"
    print "\t-l, --locator=LOCATOR\t\tlocator to connect to (defaults to localhost)"
    print "\t-p, --port=PORT\t\t\tlocator port to connect to (defaults to 10334)"
    print "\t-t, --ticket=TICKET_NUMBER\tticket number"
    print "\t-u, --upload\t\t\tUpload zip file to pivotal FTP server for the GemFire support team."
    print "\t-d, --do-not-remove-temp\t\tDon't cleanup the logs stored in the temporary location."
    print "\t-f, --ftp-server=FTPSERVER\t\tSpecify an FTP server (defaults to ftp.gemstone.com)"
    print "\t-v, --version\t\t\tPrint version of this script."
    sys.exit(1)


def print_version():
    """
    Print version and exit.
    """
    print "%s version %s" % (os.path.basename(sys.argv[0]), __version__)
    sys.exit(1)


def handle_arguments(opt):
    """
    Populate the info_container dict with all of the command line options.
    """
    for option, argument in opt:
        if option == "help" or option == "-h":
            usage()
        # Required arguments
        elif option == "--company" or option == "-c":
            info_container["company"] = argument
        elif option == "--output-dir" or option == "-o":
            info_container["output-dir"] = os.path.abspath(argument)
        # Optional arguments
        elif option == "--locator" or option == "-l":
            info_container["locator"] = argument
        elif option == "--port" or option == "-p":
            info_container["port"] = argument
        elif option == "--upload" or option == "-u":
            info_container["upload"] = False
        elif option == "--ticket" or option == "-t":
            info_container["ticket"] = argument
        elif option == "--ftp-server" or option == "-f":
            info_container["ftp-server"] = argument
        elif option == "--do-not-remove-temp" or option == "-d":
            info_container["do-not-remove-temp"] = True
        elif option == "--version" or option == "-v":
            print_version()
    if not "company" in info_container or not "output-dir" in info_container:
        usage()


def parse_options():
    """
    Check for valid input from the CLI and pass on for processing.
    """
    try:
        long_options = ["help", "version", "company=", "locator=", "port=", "do-not-upload", "ticket",
                        "output-dir", "do-not-remove-temp", "ftp-server"]
        opt, args = getopt.getopt(sys.argv[1:], "hvrduc:l:p:t:o:f:", long_options)
        handle_arguments(opt)
    except getopt.GetoptError, error:
        print "%s\n" % error
        usage()
        sys.exit(1)


def check_for_gfsh():
    """
    Ensure that 'gfsh' is in the user's PATH and save that location.
    """
    gfsh_command = "gfsh"
    found_gfsh = False
    for path in os.environ["PATH"].split(":"):
        gfsh_test_location = "%s/%s" % (path, gfsh_command)
        if os.path.isfile(gfsh_test_location):
            found_gfsh = True
            info_container["gfsh_location"] = gfsh_test_location
            if DEBUG:
                print "Using GFSH at: %s" % info_container["gfsh_location"]
            break
    if not found_gfsh:
        print "Please ensure that the gfsh command is available in your PATH"
        sys.exit(1)


def check_valid_output_dir():
    """
    Ensure that the requested output directory is valid.
    """
    if not os.path.isdir(info_container["output-dir"]):
        print "'%s' is not a valid directory." % info_container["output-dir"]
        sys.exit(1)
    if DEBUG:
        print "output dir is valid: %s" % info_container["output-dir"]


def create_tmp_dir():
    """
    Create the temporary directory to store all of the output log files.
    """
    directory_name = "%s/%s" % (info_container["output-dir"], TIMESTAMP)
    try:
        os.mkdir(directory_name)
        info_container["output-dir"] = directory_name
    except Exception, e:
        print "Unable to create directory: %s" % directory_name
        print e
        sys.exit(1)


class Gfsh:
    def __init__(self):
        """
        The GFSH class handles the interactions with the gfsh cli.
        """
        self.locator = info_container["locator"]
        self.locator_port = info_container["port"]
        self.output_dir = info_container["output-dir"]
        self.zip_filename = ""
        self.gfsh_log_dir = "-Dgfsh.log-dir=%s" % self.output_dir

    def export_logs(self):
        """
        Use the gfsh "export logs" command to grab all of the log files from a cluster.
        """
        command  = self._connect_to_locator()
        command += ' -e "export logs --dir=%s" ' % self.output_dir
        self._execute(command)

    def export_stack_traces(self):
        """
        Use the gfsh "export stack-traces" command to get stack traces from a cluster.
        """
        command  = self._connect_to_locator()
        command += ' -e "export stack-traces --file=%s/stack_traces.txt" ' % self.output_dir
        self._execute(command)

    def export_config(self):
        """
        Use the gfsh "export config" command to get the configs from all members.
        """
        command  = self._connect_to_locator()
        command += ' -e "export config --dir=%s" ' % self.output_dir
        self._execute(command)

    def create_zip_file(self):
        """
        Create the zip file from all of the files in the output directory.
        """
        zipf = self._open_zip()
        try:
            for root, dirs, files in os.walk(self.output_dir):
                for f in files:
                    if not f.endswith(".zip"):
                        zipf.write(os.path.join(root, f), f)
        except Exception, e:
            print "Error creating zip file."
            print e
            sys.exit(1)

    def send_zip_to_support(self):
        """
        If the user requests, automatically update the zip file that was generated to the
        FTP_SERVER.  Restrictions on the FTP_SERVER ensure that once this file is uploaded only
        the Pivotal support teams can download this file even though it's uploaded as an
        anonymous user.
        """
        ftp_command = 'STOR /incoming/%s' % os.path.basename(self.zip_filename)
        try:
            ftp = self._get_ftp_connection()
            ftp.storbinary(ftp_command, open(self.zip_filename, 'rb'))
            print "%s successfully uploaded to %s " % (self.zip_filename, info_container["ftp-server"])
        except Exception, e:
            print "Unable upload file.  Please provide this information to support."
            print e
            sys.exit(1)

    def do_cleanup(self):
        """
        Go through various cleanup tasks.  Delete all of the temporary files, if the user asks.

        This does not delete the created zip file.
        """
        if "upload" in info_container:
            self.send_zip_to_support()
        else:
            print "Zip file created at: %s" % self._get_zipfile_location()
        if not "do-not-remove-temp" in info_container:
            print "Cleaning up temporary files in %s " % self.output_dir
            if os.path.isdir(self.output_dir):
                for f in os.listdir(self.output_dir):
                    if self._check_file_extension(f):
                        os.remove("%s/%s" % (self.output_dir, f))

    @staticmethod
    def _check_file_extension(filename):
        """
        When cleaning up, ensure that we only clean log files that we've generated.
        :return: true if filename ends with one of our defined extension types.
        """
        if filename.endswith(".log"):
            return True
        elif filename.endswith(".gfs"):
            return True
        elif filename.endswith(".xml"):
            return True
        elif filename.endswith(".properties"):
            return True
        elif filename.endswith(".txt"):
            return True
        return False

    @staticmethod
    def _get_ftp_connection():
        """
        Try and connect to the FTP_SERVER.
        TODO: Implement proxy support
        """
        ftp = ftplib.FTP(info_container["ftp-server"])
        try:
            ftp.login()
        except Exception, e:
            print "Unable to connect to the ftp server.  Please try again later"
            print e
            sys.exit(1)
        return ftp

    def _get_zipfile_location(self):
        """
        Return the full path to the zip file that was created.
        """
        return self.zip_filename

    def _open_zip(self):
        """
        Define the name of the zip file and create the ZipFile object.
        :return: ZipFile object
        """
        self.zip_filename = "%s/%s" % (info_container["output-dir"], info_container["company"])
        if "ticket" in info_container:
            self.zip_filename += "_%s" % info_container["ticket"]
        self.zip_filename += "_%s.zip" % TIMESTAMP
        zipf = zipfile.ZipFile(self.zip_filename, 'w', zipfile.ZIP_DEFLATED)
        if DEBUG:
            print "zip filename: %s" % self.zip_filename
        return zipf

    def _connect_to_locator(self):
        """
        Defines the common command for connecting gfsh to the locator.  This command is used in conjunction
        with other commands that require gfsh being connected to a locator.
        :return: command string for connecting to the locator
        """
        command  = "JAVA_ARGS=%s " % self.gfsh_log_dir
        command += info_container["gfsh_location"]
        command += ' -e "connect '
        command += ' --locator=%s[%s]"' % (self.locator, self.locator_port)
        return command

    def _execute(self, command):
        """
        Execute the actual gfsh command.
        """
        results = commands.getstatusoutput(command)

        # gfsh seems to always exit 0...
        if results[0] != 0 or self._check_for_errors(results[1]):
            print "Something went wrong..."
            print results[1]
            sys.exit(1)
        if DEBUG:
            print "gfsh command: %s " % command
            print "gfsh command status:%s " % results[1]

    @staticmethod
    def _check_for_errors(results):
        error_strings = ["Could not connect to", "is not valid or is currently not available"]
        for error in error_strings:
            if error in results:
                return True
        return False


def main():
    parse_options()
    check_valid_output_dir()
    check_for_gfsh()
    create_tmp_dir()

    g = Gfsh()
    g.export_logs()
    g.export_config()
    g.export_stack_traces()
    g.create_zip_file()
    g.do_cleanup()


if __name__ == '__main__':
    main()