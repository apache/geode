package com.pivotal.gemfire.sendlogs.driver;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.log4j.Logger;

import com.pivotal.gemfire.sendlogs.utilities.CloseGracefully;
import com.pivotal.gemfire.sendlogs.utilities.SendlogsExceptionHandler;

public class CollectLogsFromFileMap {
	private final Logger logger = Logger.getLogger(CollectLogsFromFileMap.class.getName());
	private final File configFile;
	
	/**
	 * Parse user configuration.
	 * @param configurationFile User defined configuration file mapping hostnames to directories.
	 */
	public CollectLogsFromFileMap(File configurationFile) {
		this.configFile = configurationFile;
	}
	
	/**
	 *  Get a list of hostnames and directories from user created file.
	 *  This should be in the format:
	 *  hostname:/dir/to/grab/logs/from
	 */
	public HashMap<String, HashSet<String>> collectLogLocations() {
		BufferedReader br = null;
		
		HashMap<String, HashSet<String>> hostAndFile = new HashMap<String, HashSet<String>>();
		String line;
		try {
			br = new BufferedReader(new FileReader(configFile));
			while ((line = br.readLine()) != null) {
				String[] hostStringSplit = line.split(":");
				if (hostStringSplit.length < 2 ) {
				  if(!(line.equals("") || line.length()==0)){
					logger.warn("Encountered invalid line: " + line);
				  }
				continue;
				}
				
				// If this host has already had a directory defined, add the
				// new one to the list of directories 
				HashSet<String> al = hostAndFile.get(hostStringSplit[0]);
				if (al == null) {
					hostAndFile.put(hostStringSplit[0], new HashSet<String>());
					hostAndFile.get(hostStringSplit[0]).add(hostStringSplit[1]);
				} else {
					al.add(hostStringSplit[1]);
				}
			}
		} catch (IOException e) {
			SendlogsExceptionHandler.handleException(e);
		} finally {
			if (br != null) {
				CloseGracefully.close(br);
			}
		}
		return hostAndFile;
	}

}
