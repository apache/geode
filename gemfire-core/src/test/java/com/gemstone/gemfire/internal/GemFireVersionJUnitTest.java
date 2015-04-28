/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.experimental.categories.Category;

import com.gemstone.junit.UnitTest;

import junit.framework.TestCase;

/**
 * This test prints out the version information obtained from the
 * {@link GemFireVersion} class.  It provides a record of what version
 * of GemFire (and the JDK) was used to run the unit tests.
 */
@Category(UnitTest.class)
public class GemFireVersionJUnitTest extends TestCase {

  /**
   * Prints both the GemFire version info and the system properties.
   * We have to print both 
   */
  public void testPrintInfo() {
	ByteArrayOutputStream baos = new ByteArrayOutputStream();
	PrintStream ps = new PrintStream(baos);
    GemFireVersion.print(ps);
    final String versionOutput = baos.toString();
    System.out.println(versionOutput);
    assertTrue(versionOutput.contains("Java version:"));
    assertTrue(versionOutput.contains("Native version:"));
    assertTrue(versionOutput.contains("Source revision:"));
    assertTrue(versionOutput.contains("Source repository:"));
    assertTrue(versionOutput.contains("Running on:"));
  }

  public void testMajorMinorVersions() {
    assertEquals(1, GemFireVersion.getMajorVersion("1.0.3"));
    assertEquals(33, GemFireVersion.getMajorVersion("33.0.3"));
    
    assertEquals(7, GemFireVersion.getMinorVersion("1.7.3"));
    assertEquals(79, GemFireVersion.getMinorVersion("1.79.3"));
    assertEquals(0, GemFireVersion.getMinorVersion("1.RC1"));
    assertEquals(5, GemFireVersion.getMinorVersion("1.5Beta2"));

    assertEquals(13, GemFireVersion.getBuild("7.0.2.13"));
    assertEquals(0, GemFireVersion.getBuild("1.7.3"));
    assertEquals(0, GemFireVersion.getBuild("1.79.3"));
    assertEquals(0, GemFireVersion.getBuild("1.RC1"));
    assertEquals(0, GemFireVersion.getBuild("1.5Beta2"));

    assertTrue("7.0 should be < 7.0.2.14", GemFireVersion.compareVersions("7.0", "7.0.2.14", true) < 0);
    assertTrue("7.0.0 should be < 7.0.2.14", GemFireVersion.compareVersions("7.0.0", "7.0.2.14", true) < 0);
    assertTrue("7.0.2 should be < 7.0.2.14", GemFireVersion.compareVersions("7.0.2", "7.0.2.14", true) < 0);
    assertTrue("7.0.3 should be > 7.0.2.14", GemFireVersion.compareVersions("7.0.3", "7.0.2.14", true) > 0);
    assertTrue("7.0.1.15 should be < 7.0.2.14", GemFireVersion.compareVersions("7.0.1.15", "7.0.2.14", true) < 0);
    assertTrue("7.0.2.13 should be < 7.0.2.14", GemFireVersion.compareVersions("7.0.2.13", "7.0.2.14", true) < 0);
    assertTrue("7.0.2.14 should be > 7.0.2.13", GemFireVersion.compareVersions("7.0.2.14", "7.0.2.13", true) > 0);
    assertTrue("7.0.2.14 should be == 7.0.2.14", GemFireVersion.compareVersions("7.0.2.14", "7.0.2.14", true) == 0);
    assertTrue("7.0.2.12 should be < 7.0.2.13", GemFireVersion.compareVersions("7.0.2.12", "7.0.2.13", true) < 0);
    assertTrue("7.0.2.13 should be == 7.0.2.13", GemFireVersion.compareVersions("7.0.2.13", "7.0.2.13", true) == 0);
    assertTrue("7.0.2.15 should be > 7.0.2.13", GemFireVersion.compareVersions("7.0.2.14", "7.0.2.13", true) > 0);
  }
  
  public void testVersionClass() throws Exception {
    compare(Version.GFE_662, Version.GFE_66);
    compare(Version.GFE_6622, Version.GFE_662);
    compare(Version.GFE_71, Version.GFE_70);
    compare(Version.GFE_80, Version.GFE_70);
    compare(Version.GFE_80, Version.GFE_71);
    compare(Version.GFE_81, Version.GFE_70);
    compare(Version.GFE_81, Version.GFE_71);
    compare(Version.GFE_81, Version.GFE_80);
  }
  
  private void compare(Version later, Version earlier) {
    assertTrue(later.compareTo(earlier) > 0);
    assertTrue(later.equals(later));
    assertTrue(later.compareTo(later) == 0);
    assertTrue(earlier.compareTo(later) < 0);

    assertTrue(later.compareTo(earlier.ordinal()) > 0);
    assertTrue(later.compareTo(later.ordinal()) == 0);
    assertTrue(earlier.compareTo(later.ordinal()) < 0);
  }
}
