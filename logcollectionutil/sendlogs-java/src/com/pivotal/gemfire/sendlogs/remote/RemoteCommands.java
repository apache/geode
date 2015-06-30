package com.pivotal.gemfire.sendlogs.remote;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.log4j.Logger;

/**
 * 
 * This relies on the {@link RemoteHost} class to do the actual work.
 *
 * @author ablakeman
 *
 */
public class RemoteCommands {
	private final RemoteHost rh;
	private static final Logger logger = Logger.getLogger(RemoteCommands.class.getName());
	
	public RemoteCommands(RemoteHost rh) {
		this.rh = rh;
	}
	
	/**
	 * @return List of all pids matching a subset of package names.
	 */
	public ArrayList<String> getGemFirePids() {
		ArrayList<String> pids = new ArrayList<String>();
		String[] output = rh.executeCommand("jps -l").split("\n");
		
        for (String line : output) {
          if (line.contains("com.vmware") || line.contains("com.gemstone") || line.contains("com.pivotal")
		|| line.contains("io.pivotal") || line.contains("gemfire") || line.contains("com.gopivotal")) {
        	  if (!(line.contains("com.pivotal.gemfire.sendlogs") 
			|| line.contains("io.pivotal.gemfire.sendlogs")) ) {
        		  String foundGfePid = line.split("\\s+")[0];
        		  logger.info("Found gemfire pid: " + foundGfePid);
        		  pids.add(foundGfePid);
        	  }
          }
	    }

		getGemFirePidsFromProcessTable(pids);

		return pids.isEmpty() ? null : pids;
	}

	/**
	* Try to find possible GemFire processes using the ps command
	* looks for gemfire.jar and classpath in output of ps.
	*
	* @param pids
	*/

	protected void getGemFirePidsFromProcessTable(ArrayList<String> pids){
		//Looking for possible PID's via the process classpath that contain gemfire.jar
		//We should consider making this optional since many add everything including 
		//the kitchen sink to the classpath even when not required
		String[] output;
		output=rh.executeCommand("ps -fa | grep java").split("\n");
		for (String line : output){
			if ( line.contains("classpath") && line.contains("gemfire.jar") ){
			  if(!(line.contains("com.pivotal.gemfire.sendlogs")||
			   line.contains("io.pivotal.gemfire.sendlogs"))){
			    String foundGfePid = line.split("\\s+")[1];
			    if(!pids.contains(foundGfePid)){
				pids.add(foundGfePid);
			 	logger.info("Found gemfire pid: " + foundGfePid);
		       }
                     }
		   }
                }
         } 
	
	/**
	 * @param pid PID of GemFire process.
	 * @return String containing the entire stack trace.  This is newline delimited.
	 */
	public String getStackTrace(String pid) {
		return rh.executeCommand("jstack -l " + pid);
	}
	
	/**
	 * Get all of the log files associated with the remote PID.  This first reads /proc/$PID/fd.
	 * From there it will look for base directories of files open from /proc/$PID/fd and then 
	 * look for valid logs in each of the directories found in this manner.
	 * @param pid PID of GemFire process.
	 * @return A HashSet containing the full filenames of all found logs or null if nothing was found.
	 */
	public HashSet<String> getLogFileNamesFromPid(String pid) {
		HashSet<String> allFiles = new HashSet<String>();
		HashSet<String> filesFromLs = new HashSet<String>();
		
		/* First get files with open file descriptors */
		String[] linesFromRemoteFdFolder = rh.executeCommand(String.format("ls -l /proc/%s/fd", pid)).split("\n");
		if (linesFromRemoteFdFolder.length > 0) {
			for (String s : linesFromRemoteFdFolder) {
				/* TODO: Figure out a better way to do this */
				if (checkFileType(s)) {
					String[] lsOutputSplit = s.split("\\s+");
					String filename = lsOutputSplit[lsOutputSplit.length -1];
					filesFromLs.add(filename);
					allFiles.add(filename);
				}
			}
			/* Then check the directories that those files were in for other logs */
			for (String extraLogFile : getlogFilesFromFdOutput(filesFromLs)) {
				allFiles.add(extraLogFile);
			}
		}
		return allFiles.isEmpty() ? null : allFiles;
	}
	
	/**
	 * This method should be re-written to provide the functionality of 
	 * {@link com.pivotal.gemfire.sendlogs.remote.RemoteCommands#getLogFileNamesFromPid(String)}.  Given the a base
     * directory name return a HashSet<String> of files that we're interested in collecting.
	 * @param dirName Directory name in which to look for interesting log files.
	 * @return HashSet<String> that includes all of the files that were matched in {@link com.pivotal.gemfire.sendlogs.remote.RemoteCommands#checkFileType}
	 */
	public HashSet<String> getLogFileNamesFromDirectory(String dirName) {
		HashSet<String> allFiles = new HashSet<String>();
		String[] dirContents = rh.executeCommand(String.format("ls -1 " + dirName)).split("\n");
		if (dirContents.length > 0) {
			for (String filename : dirContents) {
				if (checkFileType(filename)) {
					allFiles.add(dirName + "/" + filename);
				}
			}
		}
		return allFiles.isEmpty() ? null : allFiles;
	}
	
	/**
	 * Based on the extension of the file, determine whether or not it should be collected to send to support.
	 * @param fullPath Full path of file to determine whether or not this is an interesting file that should be
     *                  collected.
	 * @return True if the file is of an interesting type to support.
	 */
	public boolean checkFileType(String fullPath) {
        return fullPath.toLowerCase().endsWith(".log")
                || fullPath.toLowerCase().endsWith(".err")
                || fullPath.toLowerCase().endsWith(".cfg")
                || fullPath.toLowerCase().endsWith(".gfs")
                || fullPath.toLowerCase().endsWith(".stack")
                || fullPath.toLowerCase().endsWith(".xml")
                || fullPath.toLowerCase().endsWith(".properties")
                || fullPath.toLowerCase().endsWith(".txt");
    }
	
	/**
	 * @param fullFileName Complete filename of a file including it's path.  EG. /export/foo/bar/test.txt
	 * @return String containing the base path of the file passed in.  EG. passing in /export/foo/bar/baz/test.txt 
	 * will return /export/foo/bar/baz
	 */
	public String getBaseDir(String fullFileName) {
		File f = new File(fullFileName);
		return f.getParent();
	}
	
	/**
	 * Used by {@link com.pivotal.gemfire.sendlogs.remote.RemoteCommands#getLogFileNamesFromPid} to get log files (and
     * anything else defined by {@link com.pivotal.gemfire.sendlogs.remote.RemoteCommands#checkFileType})
	 * 
	 * @param filesFromLs Files from the LS output of the 'fd' folder for a given PID.  This takes that 'ls' outpoint which
     *                    will have a symlink to the original log folder and gets the base directory name from that
     *                    symlink.
	 * @return HashSet containing a list of all of the files found that aren't currently open but are in a "log" directory.
	 */
	private HashSet<String> getlogFilesFromFdOutput(HashSet<String> filesFromLs) {
		HashSet<String> nonOpenLogs = new HashSet<String>();
		if (!filesFromLs.isEmpty()) {
			for (String fullFileName : filesFromLs) {
				String baseDirOfFile = getBaseDir(fullFileName);
				String[] linesFromRemoteFdFolder = rh.executeCommand("ls " + baseDirOfFile).split("\n");
				for (String line: linesFromRemoteFdFolder) {
					if (checkFileType(line)) {
						/* the LS returns just the filename so re-append the full PATH to the file */
						nonOpenLogs.add(baseDirOfFile + "/"+ line);
					}
				}
			}
		}
		return nonOpenLogs;
	}
}
