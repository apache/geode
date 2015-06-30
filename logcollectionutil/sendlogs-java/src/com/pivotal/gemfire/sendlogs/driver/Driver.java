package com.pivotal.gemfire.sendlogs.driver;

import java.io.Console;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.pivotal.gemfire.sendlogs.remote.FTPUploader;
import com.pivotal.gemfire.sendlogs.remote.RemoteHost;
import com.pivotal.gemfire.sendlogs.utilities.CliParser;
import com.pivotal.gemfire.sendlogs.utilities.CloseGracefully;
import com.pivotal.gemfire.sendlogs.utilities.CompressArtifacts;
import com.pivotal.gemfire.sendlogs.utilities.FileSystem;
import com.pivotal.gemfire.sendlogs.utilities.Log;
import com.pivotal.gemfire.sendlogs.utilities.SendlogsExceptionHandler;
import com.pivotal.gemfire.sendlogs.utilities.Usage;

/**
 * Primary driver class for the entire application.  Does some initial verification and setup
 * of the environment and then uses the {@link com.pivotal.gemfire.sendlogs.driver.GemFireHost} class to do all of the actual
 * work.
 * 
 * TODO: This should really be broken out into different classes.
 * 
 * @author ablakeman
 */
public class Driver {
	static CliParser options = null;
	private static String passwordFromUser = null;
	private enum AuthMethod { PASSWORD, IDENTITY, DEFAULT }
	private static File pubkeyLocation;
	private static String baseDir;
	private static AuthMethod authMethod = null;
	private static ArrayList<String> hosts;
	private static String zipFilename;
	private static boolean haveFilesBeenCollected = false;
	private static final Logger logger = Logger.getLogger(Driver.class.getName());
	private static final String OUTPUTLOGFILENAME = "sendlog.out";
	private static final long MAXZIPFILESIZEINBYTES = 2L * 1024 * 1024 * 1024; /* 2GB */
	private static String username;
	private static CompressArtifacts zipFile;

	
	public static void main(String args[]) {
		if (args.length < 1) {
			Usage.printUsage();
			CloseGracefully.exitApp();
		}
		options = new CliParser(args);
		baseDir = createAndGetBaseDir();
		setupLogging(getLogFilename());
		setAuthMethod();
		username = options.getUsername();
		hosts = new ArrayList<String>();
		zipFile = new CompressArtifacts(baseDir, options.getCustomerName(), options.getTicketNumber());
		zipFilename = zipFile.getZipFilename();
		
		/* If the user has provided a config file with hosts and directories use that*/
		if (options.staticCopyMode()) {
			collectLogs();
		} else {
		/* If not then proceed with the default scanning for PIDs and getting directories */
			findAndCollectLogs();
		}
		
		zipFile.addFileToZip(getLogFilename(), "sendlogs");
		zipFile.close();
		
		if (haveFilesBeenCollected) {
			if (options.shouldRemoveTempFiles()) {
				cleanupTmpFiles();
			}
			if (options.shouldUploadZipToPivotal()) {
				if (!sendZipToPivotal()) {
					logger.error("Unable to upload the file to "+ options.getFtpServer() +".");
				} else {
					logger.info("Upload complete.");
				}
			}
			logger.info("Logs available at: " + zipFilename);
		} else {
			logger.error("Unable to find or access any gemfire processes on the given hosts.");
		}
	}

	/**
	 * Proceed in the default manner of going to each host, looking for a gemfire process, and finding the 
	 * logs associated with that process.
	 */
	private static void findAndCollectLogs() {
		for (String host : options.getAddresses()) {
			hosts.add(host);
			logger.info("Collecting logs from: " + host);
			
			GemFireHost gfh = new GemFireHost(getRemoteHost(host), baseDir, zipFile);
			gfh.findAndRetrieveLogs();
			
			if (gfh.haveFilesBeenCollected()) {
				haveFilesBeenCollected = true;
			} else {
				logger.warn("No Gemfire proceses found on " + host);
			}
			gfh.close();
		}
	}
	
	/**
	 * Collect logs from user defined locations.  In this situation the user has chosen to
	 * provide a file with a mapping of hosts:/log/directories instead of allowing this 
	 * utility to scan for PIDs and get log files.
	 */
	private static void collectLogs() {
		File logFileFromUser = new File(options.getStaticFileLocation());
		
		if (!logFileFromUser.exists()) {
			CloseGracefully.exitApp("Unable to open configuration file: " + logFileFromUser.getPath());
		}
		
		CollectLogsFromFileMap logsFromFile = new CollectLogsFromFileMap(logFileFromUser);
		HashMap<String, HashSet<String>> hostsAndDirectories = logsFromFile.collectLogLocations();
		
		for (String host : hostsAndDirectories.keySet()) {
			hosts.add(host);
			logger.info("Collecting logs from: " + host);
			
			GemFireHost gfh = new GemFireHost(getRemoteHost(host), baseDir, zipFile);
			gfh.retrieveLogs(hostsAndDirectories.get(host));
			
			if (gfh.haveFilesBeenCollected() && gfh.getMissedDirectories().isEmpty()) {
				haveFilesBeenCollected = true;
			} else {
				logger.warn("Unable to collect logs from the following locations on " + host);
				for (String dir : gfh.getMissedDirectories()) {
					logger.warn(dir);
				}
			}
			gfh.close();
		}
	}

	
	/**
	 * Get {@link RemoteHost} object for specified authentication method 
	 * @param host - hostname of the machine
	 * @return {@link RemoteHost}
	 */
	private static RemoteHost getRemoteHost(String host) {
		RemoteHost rh;
		switch(authMethod) {
		case PASSWORD: rh = new RemoteHost(username, host, passwordFromUser);
					   break;
		case IDENTITY: rh = new RemoteHost(username, host, pubkeyLocation);
					   break;
		      default: rh = new RemoteHost(username, host);
		      		   break;
		}
		return rh;
	}
	
	/**
	 * Set up logging for the app.
	 */
	private static void setupLogging(String logFilename) {
	    if (options.debugMode()) {
	    	Log.setupLogging(logFilename, Level.DEBUG);
	    } else {
	    	Log.setupLogging(logFilename, Level.INFO);
	    }
	}
	
	/**
	 * Return the output log file name to store results of this run to send to support.
	 */
	private static String getLogFilename() {
		return baseDir + "/" + OUTPUTLOGFILENAME;
	}

	/**
	 * Send the zip file with all of the logs to the pivotal support FTP server.
	 */
	private static boolean sendZipToPivotal() {
		FTPUploader ftpU = new FTPUploader(options.getFtpServer());
		File zipFileToUpload = new File(zipFilename);
		logger.debug("max zip file size: " + MAXZIPFILESIZEINBYTES);
		logger.debug("zip length: " + zipFileToUpload.length());
		if (zipFileToUpload.length() > MAXZIPFILESIZEINBYTES) {
			logger.error("Unable to upload zip file as it's larger than 2GB.  Please contact support.");
			return false;
		}
		logger.info("Uploading logs to " + options.getFtpServer());
		
		return ftpU.sendFile(zipFilename);
	}

	/**
	 * Deletes all of the files copied over, unless the user passed the option that
	 * says not to.
	 */
	private static void cleanupTmpFiles() {
        /* If the baseDirectory isn't an actual directory return.  This ends up being redundant but is safer. */
        if (!(new File(baseDir).isDirectory())) return;

		for (String host : hosts) {
            /* If somehow host is null, some sort of whitespace, or empty, return to be safe. */
            if (host == null || host.trim().equals("")) return;
			File hostDir = new File(baseDir + "/" + host);
			if (hostDir.isDirectory()) {
				try {
					FileUtils.deleteDirectory(hostDir);
				} catch (IOException ioe) {
					try {
						// Try once more in case the first attempt didn't finish because of open files.
						FileUtils.deleteDirectory(hostDir);
					} catch (IOException e) {
						SendlogsExceptionHandler.handleException(e);
					}
				}
			}
		}
	}

	/**
	 * Figure out what Authentication method should be used for SSH connections.  Either use a password,
	 * user specified identity file, or standard identity files.
	 */
	private static void setAuthMethod() {
		/* Get password from user if that option was specified */
		if (options.promptForPassword()) {
			passwordFromUser = getPasswordFromUser();
			authMethod = AuthMethod.PASSWORD;
		} else if (options.hasIdentityFileFromUser()) {
			pubkeyLocation = options.getIdentityFile();
			if (pubkeyLocation == null) CloseGracefully.exitApp("Unable to read specified identityfile.");
			authMethod = AuthMethod.IDENTITY;
		} else {
			authMethod = AuthMethod.DEFAULT;
		}
	}

	/**
	 * Retrieve password from user.
	 * @return String that the user entered at the prompt.
	 */
	private static String getPasswordFromUser() {
		Console console = System.console();
		return new String(console.readPassword("Enter password for SSH: "));
	}

	/**
	 * Create a temporary directory in which to store all of the collected log files, etc. 
	 * @return Full pathname of the created directory.
	 */
	private static String createAndGetBaseDir() {
		FileSystem fs = new FileSystem(options.getOutputDir());
		fs.validateOrCreateBaseDirectory();
		if (!fs.createTemporaryDirectory()) {
			CloseGracefully.exitApp("Unable to create directory temporary directory: "
					+ fs.getTempLogDir() + " Check permissions.");
		}
		return fs.getTempLogDir();
	}
}
