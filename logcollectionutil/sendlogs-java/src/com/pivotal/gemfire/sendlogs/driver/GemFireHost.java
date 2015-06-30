package com.pivotal.gemfire.sendlogs.driver;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;

import com.pivotal.gemfire.sendlogs.remote.RemoteCommands;
import com.pivotal.gemfire.sendlogs.remote.RemoteHost;
import com.pivotal.gemfire.sendlogs.utilities.CompressArtifacts;
import com.pivotal.gemfire.sendlogs.utilities.FileSystem;
import com.pivotal.gemfire.sendlogs.utilities.SendlogsExceptionHandler;
/**
 * This class handles the actual collection of logs from GemFire hosts.  There
 * should be one instance of this class per GemFire host.
 * 
 * @author ablakema
 */
public class GemFireHost {
	private final RemoteHost rh;
	private final String workingDir;
	private final RemoteCommands rc;
	private final String hostname;
	private final CompressArtifacts zipFile;
	private ArrayList<String> missedDirs = new ArrayList<String>();
	private boolean haveFilesBeenCollected = false;
	private static final Logger logger = Logger.getLogger(GemFireHost.class.getName());


	/**
	 * Sets up the required environment to perform operations on a remote host
	 * and collect files.
	 * 
	 * @param rh {@link com.pivotal.gemfire.sendlogs.remote.RemoteHost} object for remote host actions.
     * @param topLevelDir Top level working directory
     * @param zipFile {@link com.pivotal.gemfire.sendlogs.utilities.CompressArtifacts} to add copied files to.
	 */
	public GemFireHost(RemoteHost rh, String topLevelDir, CompressArtifacts zipFile) {
		this.rh = rh;
		this.zipFile = zipFile;
		this.hostname = rh.getHostname();
		workingDir = String.format("%s/%s", topLevelDir, hostname);
		rc = new RemoteCommands(rh);
		createWorkingDirectory(workingDir);
	}

	/**
	 * Does all of the actual work for this class.  SCP's log files,
	 * and adds each file to the zip file.
	 * 
	 * This *could* probably be written to redirect the SCPInputStream directly to the ZipFile.
	 * 
	 */
	public void findAndRetrieveLogs() {
		ArrayList<String> pidsFromHost = rc.getGemFirePids();
		/* Verify that we have some PIDs to work with */
		if (pidsFromHost == null || pidsFromHost.isEmpty()) return;
		
		for (String pid : pidsFromHost) {
			dumpStacksToFile(pid);
			collectLogFilesFromPidAndWriteToZip(pid);
		}
	}
	
	/**
	 * @return An ArrayList containing directories that either didn't exist or 
	 * directories in which we didn't find any files we are interested in.
	 */
	public ArrayList<String> getMissedDirectories() {
		return missedDirs;
	}
	
	/**
	 * Retrieve files from the specified directory
	 */
	public void retrieveLogs(HashSet<String> directories) {
		for (String dirName: directories) {
			collectLogFilesFromDirAndWriteToZip(dirName);
		}
	}

	/**
	 * @return True if log files have been found and collected.
	 */
	public boolean haveFilesBeenCollected() {
		return haveFilesBeenCollected;
	}
	
	/**
	 * Close the SSH connection to the remote host.
	 */
	public void close() {
		rh.closeConnection();
	}

	/**
	 * Copy the log files from the remote host and then add them to the zip file.
	 * @param pid PID of the GemFire process.
	 */
	private void collectLogFilesFromPidAndWriteToZip(String pid) {
		String workingDirForPid = workingDir + "/" + pid;
		createWorkingDirectory(workingDirForPid);
		
		HashSet<String> logFileNames = rc.getLogFileNamesFromPid(pid);
		if (logFileNames != null && !logFileNames.isEmpty()) {
			for (String logFile : logFileNames){
				File remoteFile = new File(logFile);
				String localFilename = workingDirForPid + "/" + remoteFile.getName();
				rh.getRemoteFile(remoteFile.getAbsolutePath(), workingDirForPid);
				zipFile.addFileToZip(localFilename, hostname + "/" + pid  );
				/* TODO: This hasn't actually verified that the above methods were successful */
				haveFilesBeenCollected = true;
			}
		}
	}
	
	/**
	 * Copy the log files from the remote host and then add them to the zip file.
	 * @param baseDirectory Base working directory for log copies and such.
	 */
	private void collectLogFilesFromDirAndWriteToZip(String baseDirectory) {
		boolean foundLogsInThisDir = false;
		String workingDirForBaseDir = workingDir + "/" + baseDirectory;
		createWorkingDirectory(workingDirForBaseDir);
		logger.debug("Working directory for " + hostname + " " + workingDirForBaseDir);
		
		HashSet<String> logFileNames = rc.getLogFileNamesFromDirectory(baseDirectory);
		if (logFileNames != null && !logFileNames.isEmpty()) {
			for (String logFile : logFileNames){
				File lf = new File(logFile);
				logger.debug("Trying to get " + logFile + " from " + hostname);
				rh.getRemoteFile(logFile, workingDirForBaseDir);
				logger.debug("Trying to zip: " + workingDirForBaseDir + "/" + lf.getName());
				zipFile.addFileToZip(workingDirForBaseDir + "/" + lf.getName(), hostname + "/" + lf.getParent());
				/* TODO: This hasn't actually verified that the above methods were successful */
				haveFilesBeenCollected = true;
				foundLogsInThisDir = true;
			}
		}
		
		if (Boolean.FALSE.equals(foundLogsInThisDir)) {
			missedDirs.add(baseDirectory);
		}
	}
	
	/**
	 * Writes the stack of a given PID to a file.  Filename is in the format pid.stack
	 * @param pid PID of the GemFire process.
	 */
	private void dumpStacksToFile(String pid) {
		String stackFileName = workingDir + "/" + pid + ".stack";
		File stackFile = new File(stackFileName);
		try {
			FileUtils.writeStringToFile(stackFile, rc.getStackTrace(pid));
			zipFile.addFileToZip(stackFile.getAbsolutePath(), hostname);
			/* TODO: This hasn't actually verified that the above methods were successful */
			haveFilesBeenCollected = true;
		} catch (IOException e) {
			SendlogsExceptionHandler.handleException(e);
		}
	}

	/**
	 * Creates a working directory for this host.  The base level working directory is 
	 * related to the hostname of the server.  Additional directories for each PID are then
	 * created.  This is is all relative to the working directory defined in {@link Driver}
	 */
	private void createWorkingDirectory(String dirName) {
		FileSystem.createDirectory(dirName);
	}
}
