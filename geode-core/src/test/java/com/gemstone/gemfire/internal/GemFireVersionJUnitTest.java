/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal;

import static org.junit.Assert.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * This test prints out the version information obtained from the
 * {@link GemFireVersion} class.  It provides a record of what version
 * of GemFire (and the JDK) was used to run the unit tests.
 */
@Category(UnitTest.class)
public class GemFireVersionJUnitTest {

  /**
   * Prints both the GemFire version info and the system properties.
   * We have to print both 
   */
  @Test
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

  @Test
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

  @Test
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
