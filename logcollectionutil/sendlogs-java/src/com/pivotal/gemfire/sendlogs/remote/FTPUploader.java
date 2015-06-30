package com.pivotal.gemfire.sendlogs.remote;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.net.ftp.FTPClient;

import com.pivotal.gemfire.sendlogs.utilities.SendlogsExceptionHandler;

/**
 * Uploads a file to the GemStone FTP server.
 * 
 * TODO: 1. Finish proxy support. 
 * 
 * @author ablakema
 */
public class FTPUploader {
	private String ftpServer;
	private final FTPClient ftp;

    public FTPUploader(String ftpServer) {
        int FTPTIMEOUTINMS = 30000;

        this.ftpServer = ftpServer;
		ftp = new FTPClient();
        ftp.setConnectTimeout(FTPTIMEOUTINMS);
	}

	/**
	 * Upload file to remote FTP server.
	 * 
	 * @param filename
	 *            Filename of the file to upload
	 * @return True if the file was successfully uploaded.
	 */
	public boolean sendFile(String filename) {
		File zipFile = new File(filename);
		if (connect()) {
			try {
				return ftp.storeFile("incoming/" + zipFile.getName(),
						new FileInputStream(filename));
			} catch (IOException e) {
				SendlogsExceptionHandler.handleException(e);
			}
		}
		return false;
	}

	/**
	 * Check for a valid connection or connect.
	 * 
	 * @return True if there is already a valid connection or a new connection
	 *         was successfully made.
	 */
	private boolean connect() {
		boolean isConnected = false;
		try {
			if (!ftp.isConnected()) {
				ftp.connect(ftpServer);
				ftp.login("anonymous", "");
			}
			isConnected = true;
		} catch (IOException e) {
			SendlogsExceptionHandler.handleException(e);
		}
		return isConnected;
	}


}
