package com.pivotal.gemfire.sendlogs.utilities;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

public class CliParserTest {
	/** 
	 * Verify that the required options are accepted.
	 */
	@Test
	public void testRequiredOptionsWork() {
		String args[] = { "-c", "test", "-o", "blah" };
		CliParser clip = new CliParser(args);
		assertEquals("test", clip.getCustomerName());
	}
	
	/**
	 * This should throw a runtime exception that -o is unavailable.
	 */
	@Test(expected=RuntimeException.class)
	public void testRequiredOptionsAreNeeded() {
		String args[] = { "-c", "test"};
		CliParser clip = new CliParser(args);
		/* This shouldn't actually make it this far*/
		assertTrue(clip.hasUsername());
	}
	
	/**
	 * Verify that given a list of three hosts the correct List is returned.
	 */
	@Test
	public void testHostAddressList() {
		String args[] = { "-c", "test", "-o", "blah", "-a", "seiryi, titanium, elmax", "-p" };
		CliParser clip = new CliParser(args);
		List<String> l = clip.getAddresses();
		assertEquals(l.get(1).trim(), "titanium");
	}
}
