package com.pivotal.gemfire.sendlogs.utilities;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.io.IOUtils;

/**
 * Instantiating this class creates the zip file.  Files are added to the zip file via the 
 * {@link #addFileToZip(String, String)} method.  Once all of the files have been added the zip footer
 * data needs to be written by calling {@link #close}.
 * 
 * @author ablakema
 */
public class CompressArtifacts {
	private OutputStream zipOut;
	private ZipOutputStream aos;
	private final String zipFilename;

	/**
	 * Create the zip file and fill it's header.
	 * @param baseDir base directory where the zip should be stored
	 * @param customerName customer name to append to the zip filename for easy identification
	 * @param ticketNumber ticket number to append to zip filename
	 */
	public CompressArtifacts(String baseDir, String customerName, String ticketNumber) {
		zipFilename = String.format("%s/%s_%s_%s_log_artifacts.zip", baseDir, customerName, ticketNumber, Time.getTimestamp());
		try {
			zipOut = new FileOutputStream(new File(zipFilename));
			aos = new ZipOutputStream(zipOut);
		} catch (IOException e) {
			SendlogsExceptionHandler.handleException(e);
		}
	}
	
	/**
	 * Add a file to zip archive.
	 * 
	 * In the future this may be re-written to accept an input stream which could
	 * write to the zip file directly instead of using the temporary files.
	 * 
	 * @param fullFilename Local full path to the file to be zipped.
	 * @param workDir The relative directory for "inside" zip file.  
	 */
	public void addFileToZip(String fullFilename, String workDir) {
		try {
			File fullFile = new File(fullFilename);
			aos.putNextEntry(new ZipArchiveEntry(workDir + "/" + fullFile.getName()));
			IOUtils.copy(new FileInputStream(fullFile), aos);
			aos.closeEntry();
		} catch (IOException e) {
			SendlogsExceptionHandler.handleException(e);
		} 
	}
	
	/**
	 * Write out the zip footer and close the zip file.
	 */
	public void close() {
		try {
			aos.finish();
			zipOut.close();
		} catch (IOException e) {
			SendlogsExceptionHandler.handleException(e);
		}
	}
	
	/**
	 * Get the filename of the zip file.
	 * @return full path of the zip file.
	 */
	public String getZipFilename() {
		return zipFilename;
	}
}