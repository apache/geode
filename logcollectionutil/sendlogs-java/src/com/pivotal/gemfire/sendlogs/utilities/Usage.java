package com.pivotal.gemfire.sendlogs.utilities;


/**
 * Print the usage of this application.
 * This should probably be created from jopt instead of maintained separated.
 * 
 * @author ablakema
 */
public class Usage {
	
	public static void printUsage() {
		StringBuilder usage = new StringBuilder();
		usage.append("java -jar gfe-logcollect.jar -c <company> -o <output dir> [OPTIONS]\n\n");
		usage.append("Required arguments:\n");
		usage.append("\t-c company name to append to output filename\n");
		usage.append("\t-o output directory to store all collected log files\n\n");
		usage.append("Optional arguments:\n");
		usage.append("\t-a comma separated list of hosts with no spaces. EG. host1,host2,host3 (defaults to localhost)\n");
		usage.append("\t-u username to use to connect via ssh (defaults to current user)\n");
		usage.append("\t-i identity file to use for PKI based ssh (defaults to ~/.ssh/id_[dsa|rsa]\n");
		usage.append("\t-p prompt for a password to use for ssh connections\n");
		usage.append("\t-t ticket number to append to created zip file\n");
		usage.append("\t-d clean up collected log files after the zip has been created\n");
		usage.append("\t-s send the zip file to Pivotal support\n");
		usage.append("\t-f ftp server to upload collected logs to.  Defaults to ftp.gemstone.com \n");
		usage.append("\t-v print version of this utility\n");
		usage.append("\t-h print this help information\n");
		usage.append("\nStatic Copy Mode\n");
		usage.append("\t-m <file> Use a file with log locations instead of scanning for logs.\n");
		usage.append("\t   Entries should be in the format hostname:/log/location\n");
		
		System.out.println(usage.toString());
	}

}
