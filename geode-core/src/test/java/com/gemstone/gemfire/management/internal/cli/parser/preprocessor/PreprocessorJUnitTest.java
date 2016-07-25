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

import com.gemstone.gemfire.test.junit.categories.UnitTest;

/**
 * Test for Preprocessor
 */
@Category(UnitTest.class)
public class PreprocessorJUnitTest {
  
  @Test
  public void test1Arg() {
    String input = "arg1";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 1, split.length);
    assertEquals("First string", "arg1", split[0]);
  }
  
  @Test
  public void test2Args() {
    String input = "arg1?arg2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "arg1", split[0]);
    assertEquals("Second string", "arg2", split[1]);
  }
  
  @Test
  public void test1SpacedArg() {
    String input = "arg1-1 arg1-2  ";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 1, split.length);
    assertEquals("First string", "arg1-1 arg1-2", split[0]);
  }
  
  @Test
  public void test1SpacedArg1Option() {
    String input = "arg1-1 arg1-2 --option1=value1";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 3, split.length);
    assertEquals("First string", "arg1-1 arg1-2", split[0]);
    assertEquals("Second string", "--option1", split[1]);
    assertEquals("Third string", "value1", split[2]);
  }
  
  @Test
  public void test1OptionNoValue() {
    String input = "--option1";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 1, split.length);
    assertEquals("First string", "--option1", split[0]);
  }
  
  @Test
  public void test2OptionsNoValue() {
    String input = "--option1 --option2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "--option2", split[1]);
  } 
  
  @Test
  public void test2Options1Value() {
    String input = "--option1=value1 --option2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 3, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "value1", split[1]);
    assertEquals("Third string", "--option2", split[2]);
  }
  
  @Test
  public void test1OptionHasValue() {
    String input = "--option1=value1";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "value1", split[1]);
  }
  
  @Test
  public void test1Arg1OptionHasValue() {
    String input = "arg1 --option1=value1";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 3, split.length);
    assertEquals("First string", "arg1", split[0]);
    assertEquals("Second string", "--option1", split[1]);
    assertEquals("Third string", "value1", split[2]);
  }
  
  @Test
  public void test1OptionMissingValue() {
    String input = "--option1=";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "__NULL__", split[1]);
  }
  
  @Test
  public void test2OptionsMissingFirstValue() {
    String input = "--option1= --option2=value2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 4, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "__NULL__", split[1]);
    assertEquals("Third string", "--option2", split[2]);
    assertEquals("Fourth string", "value2", split[3]);
  }
  
  @Test
  public void testSingleQuotedArg() {
    String input = "\'arg1-1= arg1-2\'?arg2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "\'arg1-1= arg1-2\'", split[0]);
    assertEquals("Second string", "arg2", split[1]);
  }

  @Test
  public void testDoubleQuotedArg() {
    String input = "\"   \'arg1-1 =arg1-2   \"?arg2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "\"   \'arg1-1 =arg1-2   \"", split[0]);
    assertEquals("Second string", "arg2", split[1]);
  }
  
  @Test
  public void testSingleQuotedOption() {
    String input = "--option1=\'value1-1 =value1-2\"\' --option2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 3, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "\'value1-1 =value1-2\"\'", split[1]);
    assertEquals("Third string", "--option2", split[2]);
  }
  
  @Test
  public void testDoubleQuotedOption() {
    String input = "--option1= --option2=\"value2\"";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 4, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "__NULL__", split[1]);
    assertEquals("Third string", "--option2", split[2]);
    assertEquals("Fourth string", "\"value2\"", split[3]);
  } 

  @Test
  public void testSingleQuoteInsideDoubleQuote() {
    String input = "--option1=\"   \'  value1  \'   \"";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "\"   \'  value1  \'   \"", split[1]);
  }
  
  @Test
  public void testQuotedStringWithAdditonalData() {
    String input = "--option1=\"   \'  value1  \'   \",moreData,\"  even more data\"";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "\"   \'  value1  \'   \",moreData,\"  even more data\"", split[1]);
  }
  
  @Test
  public void testBadOption() {
    String input = "--option1=value1 -option2=value2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 4, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "value1", split[1]);
    assertEquals("Third string", "-option2", split[2]);
    assertEquals("Third string", "value2", split[3]);
  }
  
  @Test
  public void testBadOptions() {
    String input = "--option1=value1 -option3 -option4";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 4, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "value1", split[1]);
    assertEquals("Third string", "-option3", split[2]);
    assertEquals("Third string", "-option4", split[3]);
  }
  
  @Test
  public void testExtraArgSpaces() {
    String input = "   arg1?  arg2   ";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "arg1", split[0]);
    assertEquals("Second string", "arg2", split[1]);
  }
  
  @Test
  public void testExtraOptionSpaces() {
    String input = "   --option1=value1    --option2=value2   ";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 4, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "value1", split[1]);
    assertEquals("Third string", "--option2", split[2]);
    assertEquals("Fourth string", "value2", split[3]);
  }

  @Test
  public void testExtraArgAndOptionSpaces() {
    String input = "   arg1   --option1=value1   ";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 3, split.length);
    assertEquals("First string", "arg1", split[0]);
    assertEquals("Second string", "--option1", split[1]);
    assertEquals("Third string", "value1", split[2]);
  }
  
  @Test
  public void testValueSpecifierAsPartOfValue() {
    String input = "--option1=-Dprop1=value1 --option2=-Dprop2=value2 --option3";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 5, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "-Dprop1=value1", split[1]);
    assertEquals("Third string", "--option2", split[2]);
    assertEquals("Fourth string", "-Dprop2=value2", split[3]);
    assertEquals("Fifth string", "--option3", split[4]);
  }
  
  @Test
  public void testMissingOption() {
    String input = "--option1=value1 value2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 3, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "value1", split[1]);
    assertEquals("Third string", "value2", split[2]);
  }
  
  @Test
  public void testUnclosedQuoteArg() {
    String input = "\"arg1-1 arg1-2 --option1=value1 --option2=value2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 1, split.length);
    assertEquals("First string", "\"arg1-1 arg1-2 --option1=value1 --option2=value2", split[0]);
  }
  
  @Test
  public void testUnclosedQuoteOption() {
    String input = "--option1=\"value1 --option2=value2";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second string", "\"value1 --option2=value2", split[1]);
  }
  
  @Test
  public void testArgWithQuotedLongOptionSpec() {
    String input = "\"--arg=value\"";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 1, split.length);
    assertEquals("First string", "\"--arg=value\"", split[0]);
  }
  
  @Test
  public void testOptionWithQuotedLongOptionSpec() {
    String input = "--option1=\"--arg=value\"";
    String[] split = Preprocessor.split(input);
    assertEquals("Size of the split", 2, split.length);
    assertEquals("First string", "--option1", split[0]);
    assertEquals("Second", "\"--arg=value\"", split[1]);
  }
}
