package com.pivotal.gemfire.sendlogs.remote;

import static org.junit.Assert.*;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RemoteHostTest {
	private final File localFile = new File("/tmp/passwd");
	
	@Before
	public void ensureFileDoesntExist() {
		if (localFile.exists()) {
			cleanUp();
		}
	}
	
	@Test
	public void testRemoteHostWithPubkey() {
		RemoteHost ack = new RemoteHost(System.getProperty("user.name"), "localhost");
	
		/* Verify that the file got copied */
		ack.getRemoteFile("/etc/passwd", "/tmp");
		assertTrue(localFile.exists());
	}
	
	@After
	public void cleanUp() {
		localFile.delete();
	}
	
}
