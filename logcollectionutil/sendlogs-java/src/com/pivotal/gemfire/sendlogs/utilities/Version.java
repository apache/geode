package com.pivotal.gemfire.sendlogs.utilities;


/**
 * Get the version of this application.
 * @author ablakema
 */
public class Version {
	private static final String VERSION = "1.0";
	
	/**
	 * @return This application's version.
	 */
	public static String getVersion() {
		return VERSION;
	}
	
	/**
	 * Print out this application's version to STDOUT.
	 */
	public static void printVersion() {
		System.out.println("gfe-sendlogs version: " + getVersion());
	}

}
