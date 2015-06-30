package com.pivotal.gemfire.sendlogs.utilities;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

/**
 * Parse the command line options passed into the application by the user.
 * 
 * Known issues:
 * 
 * 1. The usage should be built from this class rather than it's own manual "Usage" class.
 * 2. withValuesSeparatedBy() stinks.  You have to supply commands either by:
 *    -a value1 -a value2 -a value3 
 *    or 
 *    -a value1,value2,value3
 *    -a value1, value2, value3 - WILL NOT WORK.
 * 3. The short values should have corresponding long values.
 * 
 * @author ablakema
 */
public class CliParser {
	private static final String DEFAULTFTPSERVER = "ftp.gemstone.com";
	private static final Logger logger = Logger.getLogger(CliParser.class.getName());
	private final OptionSet options;
	
	/**
	 * Parse options handed in 
	 * @param argsFromUser Should be 'args' from the {@link com.pivotal.gemfire.sendlogs.driver.Driver#main(String[]) main} method.
	 */
	public CliParser(String[] argsFromUser) {
		/* required
		 * --------
		 * c - customer name
		 * o - output directory
		 * 
		 * optional
		 * ---------
		 * a - addresses
		 * u - username
		 * i - identity file
		 * h - help
		 * p - prompt for password
		 * t - ticket number
		 * v - version
		 * d - don't remove temporary files.
		 * s - send file to GemStone support.
		 * f - specify an ftp server to use (defaults to ftp.gemstone.com)
		 * 
		 *
		 * debug - enable debug output - This needs added to the "Usage" output once it's better fleshed out.
		 */
		OptionParser parser = new OptionParser();
		parser.accepts("c").withRequiredArg().describedAs("Company name");
		parser.accepts("o").withRequiredArg().describedAs("Output directory");
		parser.accepts("a").withRequiredArg().withValuesSeparatedBy(",").describedAs("");
		parser.accepts("u").withRequiredArg();
		parser.accepts("i").withRequiredArg();
		parser.accepts("h");
		parser.accepts("p");
		parser.accepts("t").withRequiredArg();
		parser.accepts("v");
		parser.accepts("d");
		parser.accepts("s");
		parser.accepts("f").withRequiredArg();
		parser.accepts("m").withRequiredArg();
		parser.accepts("debug");
	    options = parser.parse(argsFromUser);
	    
	    if (options.has("h")) {
	    	Usage.printUsage();
	    	CloseGracefully.exitApp();
	    }
	    
	    if (options.has("v")) {
	    	Version.printVersion();
	    	CloseGracefully.exitApp();
	    }
	    
	    /* Verify required options */
	    if (!options.has("c")) {
	    	exitMissingArgument("-c");
	    }
	    
	    if (!options.has("o")) {
	    	exitMissingArgument("-o");
	    }
	}
	
	/**
	 * Exit the application because of a missing required option.
	 * @param missingArgument Required option that was missing.
	 */
	private void exitMissingArgument(String missingArgument) {
    	Log.setupLogging(null, Level.INFO);
    	logger.error("Missing required option \"" + missingArgument + "\"");
    	Usage.printUsage();
		CloseGracefully.exitApp();
	}

	/**
	 * Get the list of addresses that the user supplied.
	 * @return List of addresses supplied by the user or "localhost" if none were specified.
	 */
	@SuppressWarnings("unchecked")
	public List<String> getAddresses() {
		if (!options.has("a")) return Arrays.asList("localhost");
		return (List<String>) options.valuesOf("a");
	}
	
	/**
	 * @return Output directory specified by the user.
	 */
	public String getOutputDir() {
		return (String) options.valueOf("o");
	}
	
	/**
	 * @return identity file location or null if unavailable.
	 */
	public File getIdentityFile() {
		String identityFileString = (String) options.valueOf("i");
		File identityFile = new File(identityFileString);
		if (identityFile.isFile()) {
			return identityFile;
		} else {
			return null;
		}
	}

	/**
	 * @return True if the user specified an identity file
	 */
	public boolean hasIdentityFileFromUser() {
		return options.has("i");
	}
	
	/**
	 * @return True if user specified a different username
	 */
	public boolean hasUsername() {
		return options.has("u");
	}
	
	/**
	 * @return username specified by the user or the current logged in user.
	 */
	public String getUsername() {
		return hasUsername() ? (String) options.valueOf("u") : System.getProperty("user.name");
	}
	
	/**
	 * @return Customer name from the user.
	 */
	public String getCustomerName() {
		return (String) options.valueOf("c");
	}
	
	/**
	 * @return True if debug mode has been enabled by the user.
	 */
	public boolean debugMode() {
		return options.has("debug");
	}
	
	/**
	 * @return True if the user has specified that they would like to be prompted for a password.
	 */
	public boolean promptForPassword() {
		return options.has("p");
	}
	
	/**
	 * @return Ticket number if specified by the user or "00000" as a default.
	 */
	public String getTicketNumber() {
		if (!options.has("t")) return "00000";
		return (String) options.valueOf("t");
	}
	
	/**
	 * @return Return true if the user has specified that files should be cleaned up.
	 */
	public boolean shouldRemoveTempFiles() {
		return options.has("d");
	}

	/**
	 * @return True if the user has specified that the Zip should be uploaded.
 	 */
	public boolean shouldUploadZipToPivotal() {
		return options.has("s");
	}
	
	/**
	 * @return True if the user has opted for static copy mode
	 */
	public boolean staticCopyMode() {
		return options.has("m");
	}
	
	/**
	 * @return User defined location of static host->directory mappings file.
	 */
	public String getStaticFileLocation() {
		return (String) options.valueOf("m");
	}

	/**
	 * @return String representation of the FTP server to use for connections.  Defaults to ftp.gemstone.com.
	 */
	public String getFtpServer() {
		if (options.has("f")) {
			return (String) options.valueOf("f");
		} else {
			return DEFAULTFTPSERVER;
		}
	}
}
