package com.pivotal.gemfire.sendlogs.driver;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.pivotal.gemfire.sendlogs.remote.RemoteHost;
import com.pivotal.gemfire.sendlogs.utilities.CompressArtifacts;
import com.pivotal.gemfire.sendlogs.utilities.Log;
import com.pivotal.gemfire.sendlogs.utilities.Time;

/**
 * This needs completely re-worked with mocking.
 * @author ablakema
 *
 */
public class GemFireHostTest {
	private RemoteHost rh;
	private CompressArtifacts ca;
	private GemFireHost gfh;

	private final String BASEDIR = "/tmp";
	private final String EXPECTEDOUTPUT = String.format("pivotal_12345_%s_log_artifacts.zip", Time.getTimestamp());
	private final File output = new File(BASEDIR + "/" + EXPECTEDOUTPUT);
	private final ByteArrayOutputStream outPut = new ByteArrayOutputStream();


	@Before
	public void setup() {
		/* Change STDOUT so as not to get a npe during logger.debug warnings */
	    System.setOut(new PrintStream(outPut));
		Log.setupLogging("/tmp/output", Level.FATAL);
		if (output.exists()) output.delete();
		rh = new RemoteHost(System.getProperty("user.name"), "localhost");
		ca = new CompressArtifacts(BASEDIR, "pivotal", "12345");
		
	}

	@Test
	public void gemfireHostTest() {
		gfh = new GemFireHost(rh, "/tmp", ca);
		gfh.findAndRetrieveLogs();
		gfh.close();
		assertTrue(output.exists());
	}
	
	@After
	public void cleanup() {
		File output = new File(BASEDIR + "/" + EXPECTEDOUTPUT);
		File log = new File("/tmp/output");
		log.delete();
		output.delete();
	}
}
