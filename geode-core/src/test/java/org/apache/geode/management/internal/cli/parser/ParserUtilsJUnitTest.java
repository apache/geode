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
package org.apache.geode.management.internal.cli.parser;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Includes tests for all utility methods in {@link ParserUtils}
 */
@Category(UnitTest.class)
public class ParserUtilsJUnitTest {

  /**
   * Test for {@link ParserUtils#split(String, String)}
   */
  @Test
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
  @Test
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
  @Test
  public void testContains() {
    String input = "something::{::}::nothing::";
    assertTrue("Check Boolean", ParserUtils.contains(input, "::"));
    input = "{something::{::}::nothing::}";
    assertFalse("Check Boolean", ParserUtils.contains(input, "::"));
  }

  /**
   * Test for {@link ParserUtils#lastIndexOf(String, String)}
   */
  @Test
  public void testLastIndexOf() {
    String input = "something::{::}::nothing::";
    assertEquals("lastIndex", 24, ParserUtils.lastIndexOf(input, "::"));
    input = "something::{::}::\"nothing::\"";
    assertEquals("lastIndex", 15, ParserUtils.lastIndexOf(input, "::"));
    input = "{something::{::}::\"nothing::\"}";
    assertEquals("lastIndex", -1, ParserUtils.lastIndexOf(input, "::"));
  }

}
