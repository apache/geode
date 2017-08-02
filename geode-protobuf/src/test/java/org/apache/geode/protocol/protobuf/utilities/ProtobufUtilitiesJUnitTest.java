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

package org.apache.geode.protocol.protobuf.utilities;

import com.google.protobuf.ByteString;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.utilities.exception.UnknownProtobufPrimitiveType;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.Charset;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Category(UnitTest.class)
public class ProtobufUtilitiesJUnitTest {
  @Test
  public void getIntPrimitiveFromEncodedValue() throws UnknownProtobufPrimitiveType {
    BasicTypes.EncodedValue.Builder builder = BasicTypes.EncodedValue.newBuilder();
    BasicTypes.EncodedValue encodedValue = builder.setIntResult(1).build();
    assertEquals(1, ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
  }

  @Test
  public void getLongPrimitiveFromEncodedValue() throws UnknownProtobufPrimitiveType {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setLongResult(1).build();
    assertEquals(1l, ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
  }

  @Test
  public void getShortPrimitiveFromEncodedValue() throws UnknownProtobufPrimitiveType {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setShortResult(1).build();
    assertEquals((short) 1, ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
  }

  @Test
  public void getBytePrimitiveFromEncodedValue() throws UnknownProtobufPrimitiveType {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setByteResult(1).build();
    assertEquals((byte) 1, ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
  }

  @Test
  public void getBooleanPrimitiveFromEncodedValue() throws UnknownProtobufPrimitiveType {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setBooleanResult(true).build();
    assertEquals(true, ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
  }

  @Test
  public void getDoublePrimitiveFromEncodedValue() throws UnknownProtobufPrimitiveType {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setDoubleResult(1.0).build();
    assertEquals(1.0, ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
  }

  @Test
  public void getFloatPrimitiveFromEncodedValue() throws UnknownProtobufPrimitiveType {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setFloatResult(1).build();
    assertEquals(1f, ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
  }

  @Test
  public void getByteArrayPrimitiveFromEncodedValue() throws UnknownProtobufPrimitiveType {
    BasicTypes.EncodedValue encodedValue = BasicTypes.EncodedValue.newBuilder()
        .setBinaryResult(ByteString.copyFrom("SomeBinary".getBytes())).build();
    assertArrayEquals("SomeBinary".getBytes(Charset.forName("UTF-8")),
        (byte[]) ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
  }

  @Test
  public void getStringPrimitiveFromEncodedValue() throws UnknownProtobufPrimitiveType {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setStringResult("SomeString").build();
    assertEquals("SomeString", ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
  }

  @Test
  public void doesAIntValueSuccessfullyEncodeIntoPrimitiveEncodedValues()
      throws UnknownProtobufPrimitiveType {
    createAndVerifyEncodedValue(1);
  }

  @Test
  public void doesALongValueSuccessfullyEncodeIntoPrimitiveEncodedValues()
      throws UnknownProtobufPrimitiveType {
    createAndVerifyEncodedValue(1l);
  }

  @Test
  public void doesAShortValueSuccessfullyEncodeIntoPrimitiveEncodedValues()
      throws UnknownProtobufPrimitiveType {
    createAndVerifyEncodedValue((short) 1);
  }

  @Test
  public void doesAByteValueSuccessfullyEncodeIntoPrimitiveEncodedValues()
      throws UnknownProtobufPrimitiveType {
    createAndVerifyEncodedValue((byte) 1);
  }

  @Test
  public void doesABooleanValueSuccessfullyEncodeIntoPrimitiveEncodedValues()
      throws UnknownProtobufPrimitiveType {
    createAndVerifyEncodedValue(true);
  }

  @Test
  public void doesADoubleValueSuccessfullyEncodeIntoPrimitiveEncodedValues()
      throws UnknownProtobufPrimitiveType {
    createAndVerifyEncodedValue((double) 1);
  }

  @Test
  public void doesAFloatValueSuccessfullyEncodeIntoPrimitiveEncodedValues()
      throws UnknownProtobufPrimitiveType {
    createAndVerifyEncodedValue((float) 1);
  }

  @Test
  public void doesABinaryValueSuccessfullyEncodeIntoPrimitiveEncodedValues()
      throws UnknownProtobufPrimitiveType {
    createAndVerifyEncodedValue("Some Text to Binary".getBytes());
  }

  @Test
  public void doesAStringValueSuccessfullyEncodeIntoPrimitiveEncodedValues()
      throws UnknownProtobufPrimitiveType {
    createAndVerifyEncodedValue("Some String text to test");
  }

  private <T> void createAndVerifyEncodedValue(T testObj) throws UnknownProtobufPrimitiveType {
    BasicTypes.EncodedValue encodedValue = ProtobufUtilities.createPrimitiveEncodedValue(testObj);
    if (testObj instanceof byte[]) {
      assertArrayEquals((byte[]) testObj,
          (byte[]) ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
    } else {
      assertEquals(testObj, ProtobufUtilities.getPrimitiveValueFromEncodedValue(encodedValue));
    }
  }
}
