/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.parser.preprocessor;

import org.junit.experimental.categories.Category;

import junit.framework.TestCase;

import com.gemstone.gemfire.internal.lang.SystemUtils;
import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.PreprocessorUtils;
import com.gemstone.gemfire.management.internal.cli.parser.preprocessor.TrimmedInput;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Includes tests for all utility methods in {@link PreprocessorUtils}
 * 
 * @author njadhav
 * 
 */
@Category(UnitTest.class)
public class PreprocessorUtilsJUnitTest extends TestCase {

  /**
   * Test for {@link PreprocessorUtils#simpleTrim(String)}
   */
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
  public void testRemoveWhiteSpaces() {
    String input = "1 2 3   ";
    String output = PreprocessorUtils.removeWhiteSpaces(input);
    assertEquals("Output after removing white spaces", "123", output);
  }

  /**
   * Test for {@link PreprocessorUtils#isSyntaxValid(String)}
   */
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
  public void testContainsOnlyWhiteSpaces() {
    assertTrue(PreprocessorUtils
        .containsOnlyWhiteSpaces("                                                  "));
    assertFalse(PreprocessorUtils
        .containsOnlyWhiteSpaces("              d       "));
  }

  /**
   * Test for {@link PreprocessorUtils#isWhitespace(char)}
   */
  public void testIsWhitespace() {
    assertTrue(PreprocessorUtils.isWhitespace(' '));
    assertTrue(PreprocessorUtils.isWhitespace('\t'));
    assertTrue(PreprocessorUtils.isWhitespace('\n'));
    assertEquals(SystemUtils.isWindows(), PreprocessorUtils.isWhitespace('\r'));
  }

}
