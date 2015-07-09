/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.management.internal.cli.parser.ParserUtils;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Includes tests for all utility methods in {@link ParserUtils}
 * 
 * @author njadhav
 * 
 */
@Category(UnitTest.class)
public class ParserUtilsJUnitTest extends TestCase {

  /**
   * Test for {@link ParserUtils#split(String, String)}
   */
  public void testSplit() {
    String input = "something::{::}::nothing";
    String[] split = ParserUtils.split(input, "::");
    assertEquals("Size of the split", 3, split.length);
    assertEquals("First string", "something", split[0]);
    assertEquals("Second string", "{::}", split[1]);
    assertEquals("Third string", "nothing", split[2]);
  }

  /**
   * Test for {@link ParserUtils#splitValues(String, String)}
   */
  public void testSplitValues() {
    String input = "something::{::}::nothing::";
    String[] split = ParserUtils.splitValues(input, "::");
    assertEquals("Size of the split", 4, split.length);
    assertEquals("First string", "something", split[0]);
    assertEquals("Second string", "{::}", split[1]);
    assertEquals("Third string", "nothing", split[2]);
    assertEquals("Fourth string", "", split[3]);
  }

  /**
   * Test for {@link ParserUtils#contains(String, String)}
   */
  public void testContains() {
    String input = "something::{::}::nothing::";
    assertTrue("Check Boolean", ParserUtils.contains(input, "::"));
    input = "{something::{::}::nothing::}";
    assertFalse("Check Boolean", ParserUtils.contains(input, "::"));
  }

  /**
   * Test for {@link ParserUtils#lastIndexOf(String, String)}
   */
  public void testLastIndexOf() {
    String input = "something::{::}::nothing::";
    assertEquals("lastIndex", 24, ParserUtils.lastIndexOf(input, "::"));
    input = "something::{::}::\"nothing::\"";
    assertEquals("lastIndex", 15, ParserUtils.lastIndexOf(input, "::"));
    input = "{something::{::}::\"nothing::\"}";
    assertEquals("lastIndex", -1, ParserUtils.lastIndexOf(input, "::"));
  }

}
