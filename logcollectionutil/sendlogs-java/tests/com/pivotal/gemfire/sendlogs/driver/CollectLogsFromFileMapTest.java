package com.pivotal.gemfire.sendlogs.driver;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.pivotal.gemfire.sendlogs.utilities.CloseGracefully;
import com.pivotal.gemfire.sendlogs.utilities.Log;

public class CollectLogsFromFileMapTest {
	private File fileMapLocation = new File("/tmp/CollectLogsTest.txt");
	private final String logOutput = "/tmp/junit_logs.txt";

	@Before
	public void setUpLogging() {
		FileWriter fw = null;
		Log.setupLogging(logOutput, Level.DEBUG);
		try {
			fw = new FileWriter(fileMapLocation);
			fw.write("titanium:/tmp\n");
			fw.write("titanium/foo\n");
			fw.write("elmax:/var/log\n");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (fw != null) CloseGracefully.close(fw);
		}
	}

	@Test
	public void collectLogLogcationsTest() {
		CollectLogsFromFileMap clffm = new CollectLogsFromFileMap(fileMapLocation);
		HashMap<String, HashSet<String>> testCollection = clffm.collectLogLocations();
		ArrayList<String> dirs = new ArrayList<String>();
		ArrayList<String> hosts = new ArrayList<String>();

		for (String hostname : testCollection.keySet()) {
			HashSet<String> dirsForHost = testCollection.get(hostname);
			hosts.add(hostname);
			for (String dir : dirsForHost) {
				dirs.add(dir);
			}
		}
		String s = null;
		try {
			s = FileUtils.readFileToString(new File(logOutput));
		} catch (IOException e) {
			e.printStackTrace();
		}
		if (s == null) {
			fail("Can't read log output");
		} else {
			/* Verify that we logged an error on invalid input */
			assertTrue(s.contains("Encountered invalid line: titanium/foo"));
			/* Verify that the line elmax:/var/log was processed correctly */
			assertTrue(hosts.contains("elmax"));
			assertTrue(dirs.contains("/var/log"));
		}
	}
	
	@After
	public void cleanup() {
		fileMapLocation.delete();
		new File("/tmp/junit_logs.txt").delete();
	}

}
