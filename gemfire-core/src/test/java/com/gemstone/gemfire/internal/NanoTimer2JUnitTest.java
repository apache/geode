/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.*;

/**
 * Unit tests for NanoTimer
 */
@Category(UnitTest.class)
public class NanoTimer2JUnitTest extends TestCase {

    public NanoTimer2JUnitTest(String name) {
        super(name);
    }
    ///////////////////////  Test Methods  ///////////////////////

    private void _testGetTime(int waitTimeSeconds) {
	final int nanosPerMilli = 1000000;
	long start = NanoTimer.getTime();
	long startMillis = System.currentTimeMillis();
	try {
	    Thread.sleep(waitTimeSeconds * 1000);
	} catch (InterruptedException e) {
	  fail("interrupted");
	}
	long end = NanoTimer.getTime();
	long endMillis = System.currentTimeMillis();
	long elapsed = (end - start);
	long elapsedMillis = endMillis - startMillis;
	assertApproximate("expected approximately " + waitTimeSeconds + " seconds", nanosPerMilli*30,
			  elapsedMillis * nanosPerMilli, elapsed);
    }
    public void testGetTime() {
      _testGetTime(2);
    }

    private long calculateSlop() {
	// calculate how much time this vm takes to do some basic stuff.
	long startTime = NanoTimer.getTime();
	new NanoTimer();
	assertApproximate("should never fail", 0, 0, 0);
	long result = NanoTimer.getTime() - startTime;
	return result * 3; // triple to be on the safe side
    }
    
    public void testReset() {
	final long slop = calculateSlop();
	NanoTimer nt = new NanoTimer();
	long createTime = NanoTimer.getTime();
	assertApproximate("create time", slop, 0, nt.getTimeSinceConstruction());
	assertApproximate("construction vs. reset", slop, nt.getTimeSinceConstruction(), nt.getTimeSinceReset());
	assertApproximate("time since reset time same as construct", slop, NanoTimer.getTime() - createTime, nt.getTimeSinceReset());
	assertApproximate("reset time same as construct", slop, NanoTimer.getTime() - createTime, nt.reset());
	long resetTime = NanoTimer.getTime();
	assertApproximate("reset time updated", slop, NanoTimer.getTime() - resetTime, nt.getTimeSinceReset());
    }
    
    /**
     * Checks to see if actual is within range nanos of expected.
     */
    private static void assertApproximate(String message, long range,
					  long expected, long actual) {
	if ((actual < (expected - range)) || (actual > (expected + range))) {
	    fail(message + " expected to be in the range ["
                 + (expected - range) + ".." + (expected + range)
                 + "] but was:<" + actual + ">");
	}
    }
}
