package com.pivotal.gemfire.sendlogs.utilities;

import java.io.File;


/**
 * This class handles all filesystem based operations.
 * @author ablakema
 */
public class FileSystem {
	private final String baseDir;
	private String tempDirName;
	
	public FileSystem (String baseDir) {
		this.baseDir = baseDir;
	}
	
	/** 
	 * Makes sure that the specified directory exists and creates it if not.
	 * Does not check permissions on the directory, that will be done when files
	 * are trying to be created.
	 * @return True if the base directory is created or already exists as a directory.
	 */
	public boolean validateOrCreateBaseDirectory() {
		File outputDirectory = new File(baseDir);
		if (outputDirectory.isDirectory()) {
			return true;
		} else if (outputDirectory.mkdir()) {
			return true;
		} else {
			throw new RuntimeException(String.format("Unable to create directory %s.", baseDir));
		}
	}
	
	/**
	 * Creates the temporary directory that is used as the new base to store all
	 * collected log files, etc.  This is also the directory that the zip file writes into.
	 * @return True if the temporary directory was successfully created.  Directory 
	 * is in the format: gemfire-support-164852-01-13-2014
	 */
	public boolean createTemporaryDirectory() {
		tempDirName = String.format("%s/gemfire-support-%s", baseDir, Time.getTimestamp());
		final File tempDir = new File(tempDirName);
		return tempDir.mkdirs();
	}
	
	/**
	 * 
	 * @return Full path of the temporary directory used for storing files.
	 */
	public String getTempLogDir() {
		return tempDirName;
	}
	
	/**
	 * Static class to create directories based on a string.
	 * @param dirName Directory name to create.
	 * @return True if the directory was successfully created.
	 */
	public static boolean createDirectory(String dirName) {
		File newDir = new File(dirName);
		return newDir.mkdirs();
	}
	

}
