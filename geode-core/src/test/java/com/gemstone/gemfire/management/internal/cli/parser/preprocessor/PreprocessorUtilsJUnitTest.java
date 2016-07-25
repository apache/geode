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
package com.gemstone.gemfire.management.internal.cli.parser.preprocessor;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Includes tests for all utility methods in {@link PreprocessorUtils}
 */
@Category(UnitTest.class)
public class PreprocessorUtilsJUnitTest {

  /**
   * Test for {@link PreprocessorUtils#simpleTrim(String)}
   */
  @Test
  public void testSimpleTrim() {
    String input = " 1 2 3 ";
    TrimmedInput simpleTrim = PreprocessorUtils.simpleTrim(input);
    assertEquals("No of spaces removed", 1, simpleTrim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "1 2 3", simpleTrim.getString());

    input = " 1 2 3      ";
    simpleTrim = PreprocessorUtils.simpleTrim(input);
    assertEquals("No of spaces removed", 1, simpleTrim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "1 2 3", simpleTrim.getString());
  }

  /**
   * Test for {@link PreprocessorUtils#trim(String)}
   */
  @Test
  public void testTrim() {
    String input = " command argument1 argument2 ";
    TrimmedInput trim = PreprocessorUtils.trim(input);
    assertEquals("No of spaces removed", 1, trim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "command argument1 argument2",
        trim.getString());

    input = "   command   argument1   argument2 ";
    trim = PreprocessorUtils.trim(input);
    assertEquals("No of spaces removed", 7, trim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "command argument1 argument2",
        trim.getString());

    input = "command argument1 argument2 -- -- - - - -- -- -- -- -- --- --------- - - - --- --";
    trim = PreprocessorUtils.trim(input);
    assertEquals("No of spaces removed", 0, trim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "command argument1 argument2",
        trim.getString());

    input = "command argument1 argument2 --";
    trim = PreprocessorUtils.trim(input);
    assertEquals("No of spaces removed", 0, trim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "command argument1 argument2",
        trim.getString());

    input = "command argument1 argument2 -";
    trim = PreprocessorUtils.trim(input);
    assertEquals("No of spaces removed", 0, trim.getNoOfSpacesRemoved());
    assertEquals("input after trimming", "command argument1 argument2",
        trim.getString());
  }

  /**
   * Test for {@link PreprocessorUtils#removeWhiteSpaces(String)}
   */
  @Test
  public void testRemoveWhiteSpaces() {
    String input = "1 2 3   ";
    String output = PreprocessorUtils.removeWhiteSpaces(input);
    assertEquals("Output after removing white spaces", "123", output);
  }

  /**
   * Test for {@link PreprocessorUtils#isSyntaxValid(String)}
   */
  @Test
  public void testIsSyntaxValid() {
    assertTrue(PreprocessorUtils.isSyntaxValid("{}"));
    assertFalse(PreprocessorUtils.isSyntaxValid("{{]}"));
    assertTrue(PreprocessorUtils.isSyntaxValid("\"\""));
    assertTrue(PreprocessorUtils.isSyntaxValid("\"{\'[]\'}\""));
    assertFalse(PreprocessorUtils.isSyntaxValid("{\"}\""));
  }

  /**
   * Test for {@link PreprocessorUtils#containsOnlyWhiteSpaces(String)}
   */
  @Test
  public void testContainsOnlyWhiteSpaces() {
    assertTrue(PreprocessorUtils
        .containsOnlyWhiteSpaces("                                                  "));
    assertFalse(PreprocessorUtils
        .containsOnlyWhiteSpaces("              d       "));
  }

  /**
   * Test for {@link PreprocessorUtils#isWhitespace(char)}
   */
  @Test
  public void testIsWhitespace() {
    assertTrue(PreprocessorUtils.isWhitespace(' '));
    assertTrue(PreprocessorUtils.isWhitespace('\t'));
    assertTrue(PreprocessorUtils.isWhitespace('\n'));
    assertEquals(SystemUtils.isWindows(), PreprocessorUtils.isWhitespace('\r'));
  }

}
