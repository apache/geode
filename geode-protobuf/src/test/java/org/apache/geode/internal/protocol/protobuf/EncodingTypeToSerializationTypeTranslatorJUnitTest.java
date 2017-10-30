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
package org.apache.geode.internal.protocol.protobuf;

import static org.junit.Assert.assertSame;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.protocol.serialization.SerializationType;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class EncodingTypeToSerializationTypeTranslatorJUnitTest {

  @Test
  public void testTranslateEncodingTypes() throws UnsupportedEncodingTypeException {
    assertSame(SerializationType.JSON,
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.JSON));
  }

  @Test(expected = UnsupportedEncodingTypeException.class)
  public void testTranslateInvalidEncoding_throwsException()
      throws UnsupportedEncodingTypeException {
    EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.INVALID);
  }

  @Test
  public void testAllEncodingTypeTranslations() {
    for (BasicTypes.EncodingType encodingType : BasicTypes.EncodingType.values()) {
      if (!(encodingType.equals(BasicTypes.EncodingType.UNRECOGNIZED)
          || encodingType.equals(BasicTypes.EncodingType.INVALID))) {
        try {
          EncodingTypeTranslator.getSerializationTypeForEncodingType(encodingType);
        } catch (UnsupportedEncodingTypeException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
