package com.pivotal.gemfire.sendlogs.utilities;

import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

public class Log {

	/**
	 * @param logFilename Location to store a file that contains the same output as STDOUT.  null if no file based logging is required.
	 * @param level Debugging level.
	 */
	public static void setupLogging(String logFilename, Level level) {
		/* Format of logging output:
		 * [INFO] Collecting logs from: titanium
		 * [ERROR] Authentication failed for titanium
		 */
		PatternLayout pl = new PatternLayout("[%p] %m%n");
	    Logger rootLogger = Logger.getRootLogger();
    	rootLogger.setLevel(level);
	    rootLogger.addAppender(new ConsoleAppender(pl));
	    /**
	     * Add logging output to a file, if a filename was specified.
	     */
    	if (logFilename != null) {
		    try {
				rootLogger.addAppender(new FileAppender(pl, logFilename));
			} catch (IOException e) {
				SendlogsExceptionHandler.handleException(e);
			}
    	}
	}
}
