package com.pivotal.gemfire.sendlogs.utilities;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Gracefully handle closing various things so that I don't have to litter my
 * object's finally blocks with try\catch statements. 
 * 
 * @author ablakema
 *
 */

public class CloseGracefully {
	
	/**
	 * Try to close the BufferedReader.  Prints a stack trace if unsuccessful.
	 * @param br {@link java.io.BufferedReader} that should be closed.
	 */
	public static void close(BufferedReader br) {
		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Try to close the FileWriter.  Prints a stack trace if unsuccessful.
	 * @param fw {@link java.io.FileWriter} to be closed.
	 */
	public static void close(FileWriter fw) {
		try {
			fw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Try to close the InputStream.  Prints a stack trace if unsuccessful.
	 * @param is {@link java.io.InputStream} to be closed.
	 */
	public static void close(InputStream is) {
		try {
			is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Try to close the InputStreamReader.  Prints a stack trace if unsuccessful.
	 * @param isr {@link java.io.InputStreamReader} to be closed.
	 */
	public static void close(InputStreamReader isr) {
		try {
			isr.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Exit this application with a specified message.
	 * @param message Message to print to STDOUT before exiting.
	 */
	public static void exitApp(String message) {
		System.out.println(message);
		System.exit(0);
	}
	
	/**
	 * Exit the application with no message.
	 */
	public static void exitApp() {
		System.exit(0);
	}
}
