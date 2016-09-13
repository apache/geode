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
package com.gemstone.gemfire.cache.query.internal.types;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.query.internal.parse.OQLLexerTokenTypes;
import com.gemstone.gemfire.pdx.internal.PdxString;
import com.gemstone.gemfire.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class TypeUtilTest {

  @Test
  public void comparingEquivalentPdxStringToStringShouldMatchCorrectly() throws Exception {
    String theString = "MyString";
    PdxString pdxString = new PdxString(theString);
    assertTrue((Boolean)TypeUtils.compare(pdxString, theString, OQLLexerTokenTypes.TOK_EQ));
    assertTrue((Boolean)TypeUtils.compare(theString, pdxString, OQLLexerTokenTypes.TOK_EQ));
  }

  @Test
  public void comparingUnequivalentPdxStringToStringShouldNotMatch() throws Exception {
    String theString = "MyString";
    PdxString pdxString = new PdxString("AnotherString");
    assertFalse((Boolean)TypeUtils.compare(pdxString, theString, OQLLexerTokenTypes.TOK_EQ));
    assertFalse((Boolean)TypeUtils.compare(theString, pdxString, OQLLexerTokenTypes.TOK_EQ));
  }
}
