package com.pivotal.gemfire.sendlogs.utilities;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.log4j.Logger;

/**
 * TODO: Figure out the best way to handle this...
 * @author ablakeman
 */
public class SendlogsExceptionHandler {
	private static final Logger logger = Logger.getLogger(SendlogsExceptionHandler.class.getName());

	public static void handleException(Exception e) {
		/* Needed to record full exception in log4j output */
		StringWriter errors = new StringWriter();
		e.printStackTrace(new PrintWriter(errors));
		/* Print a 1 liner to the console */
		logger.error(e);
		/* If in debug print the whole stack */
		logger.debug(errors.toString());
	}
	
	public static void handleException(String errorMesg, Exception e) {
		logger.error(errorMesg, e);
	}

}
