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
package org.apache.geode.protocol.protobuf;

import org.apache.geode.serialization.SerializationType;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertSame;

@Category(UnitTest.class)
public class EncodingTypeToSerializationTypeTranslatorJUnitTest {

  @Test
  public void testTranslateEncodingTypes() throws UnsupportedEncodingTypeException {
    assertSame(SerializationType.INT,
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.INT));
    assertSame(SerializationType.LONG,
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.LONG));
    assertSame(SerializationType.SHORT,
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.SHORT));
    assertSame(SerializationType.BYTE,
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.BYTE));
    assertSame(SerializationType.BOOLEAN, EncodingTypeTranslator
        .getSerializationTypeForEncodingType(BasicTypes.EncodingType.BOOLEAN));
    assertSame(SerializationType.BINARY,
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.BINARY));
    assertSame(SerializationType.FLOAT,
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.FLOAT));
    assertSame(SerializationType.DOUBLE,
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.DOUBLE));
    assertSame(SerializationType.STRING,
        EncodingTypeTranslator.getSerializationTypeForEncodingType(BasicTypes.EncodingType.STRING));
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
