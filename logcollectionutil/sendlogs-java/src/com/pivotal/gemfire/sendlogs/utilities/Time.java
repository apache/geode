package com.pivotal.gemfire.sendlogs.utilities;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Time operations for various classes.
 * @author ablakema
 */
public class Time {
	private static final String DATE_FORMAT = "HHmmss-MM-dd-yyyy";
	
	/**
	 * Get a timestamp based on the current date.
	 * This includes minutes and seconds so that the user can run multiple times if need be.
	 * @return String with the timestamp.
	 */
	public static String getTimestamp() {
      SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT);
      Calendar cal = Calendar.getInstance();
      return sdf.format(cal.getTime());
	}
}
