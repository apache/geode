package com.pivotal.gemfire.sendlogs.remote;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.security.SecureRandom;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FTPUploaderTest {
	private String testFileToUpload;
	File fileToCreate;
	
	/**
	 * Create a file to upload
	 */
	@Before
	public void setUpTestFile() {
        SecureRandom random = new SecureRandom();
        long randomNumber = random.nextLong();
		testFileToUpload = "sendlogs-" + randomNumber;
		fileToCreate = new File(testFileToUpload);
		try {
			fileToCreate.createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * This will actually upload a file to our FTP server.  Anonymous users
	 * can't delete this so it will either have to be manually removed or 
	 * it will be automatically cleaned up after 15 days.
	 */
	@Test
	public void testSendingFile() {
		FTPUploader ftpu = new FTPUploader("ftp.gemstone.com");
		assertEquals(true, ftpu.sendFile(testFileToUpload));
	}
	
	@After
	public void cleanup() {
		fileToCreate.delete();
	}

}
