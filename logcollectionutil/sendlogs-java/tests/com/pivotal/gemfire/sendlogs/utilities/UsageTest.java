package com.pivotal.gemfire.sendlogs.utilities;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UsageTest {
	
	/**
	 * Change stdout so that we can compare what's printed to the console more easily
	 */
	private final ByteArrayOutputStream outPut = new ByteArrayOutputStream();
	private final ByteArrayOutputStream errOut = new ByteArrayOutputStream();

	@Before
	public void setUpStreams() {
	    System.setOut(new PrintStream(outPut));
	    System.setErr(new PrintStream(errOut));
	}

	@Test
	public void test() {
		Usage.printUsage();
		assertTrue(outPut.toString().contains("gfe-sendlogs -c <company> -o <output dir> [OPTIONS]\n\n"));
	}

	@After
	public void cleanUpStreams() {
	    System.setOut(null);
	    System.setErr(null);
	}
}
