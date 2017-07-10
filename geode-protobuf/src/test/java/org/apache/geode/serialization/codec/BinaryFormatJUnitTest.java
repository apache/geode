/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.serialization.codec;

import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.Charset;

import static org.junit.Assert.*;

@Category(UnitTest.class)
public class BinaryFormatJUnitTest {
  private static final Charset UTF8 = Charset.forName("UTF-8");
  private static final Charset UTF16 = Charset.forName("UTF-16");
  private String testString = "Test String";

  private StringCodec stringCodec;

  @Before
  public void startup() {
    stringCodec = new StringCodec();
  }

  @Test
  public void testStringsUseUTF8Encoding() {
    assertArrayEquals(testString.getBytes(UTF8), stringCodec.encode(testString));
  }

  @Test
  public void testStringDontUseUTF16Encoding() {
    byte[] expectedEncodedString = stringCodec.encode(testString);
    byte[] incorrectEncodedString = testString.getBytes(UTF16);
    assertNotEquals(expectedEncodedString.length, incorrectEncodedString.length);
  }

  @Test
  public void testImproperlyEncodedStringDecodingFails() {
    byte[] encodedString = testString.getBytes(UTF16);
    assertNotEquals(testString, stringCodec.decode(encodedString));
  }

  @Test
  public void testProperlyEncodedStringDecoding() {
    byte[] encodedString = testString.getBytes(UTF8);
    assertEquals(testString, stringCodec.decode(encodedString));
  }
}
