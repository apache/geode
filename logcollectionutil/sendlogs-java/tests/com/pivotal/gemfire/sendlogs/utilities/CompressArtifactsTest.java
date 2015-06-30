package com.pivotal.gemfire.sendlogs.utilities;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Test;

public class CompressArtifactsTest {
	private final String outputFilename = String.format("pivotal_12345_%s_log_artifacts.zip", Time.getTimestamp());

	@Test
	public void testArtifactCompression() {
		final String baseDir = "/tmp";
		final String customerName = "pivotal";
		final String ticketNumber = "12345";
		CompressArtifacts ca = new CompressArtifacts(baseDir, customerName, ticketNumber);
		ca.addFileToZip("/etc/passwd", "junit");
		ca.close();
		assertTrue(new File("/tmp/" + outputFilename).exists());
	}
	
	@After
	public void cleanUp() {
		File f = new File(outputFilename);
		f.delete();
	}

}
