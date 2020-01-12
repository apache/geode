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

package org.apache.geode.internal.protocol.protobuf.v1.utilities;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.charset.Charset;

import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.test.junit.categories.ClientServerTest;

@Category({ClientServerTest.class})
public class ProtobufUtilitiesJUnitTest {

  private ProtobufSerializationService protobufSerializationService =
      new ProtobufSerializationService();;

  @Test
  public void getIntPrimitiveFromEncodedValue() throws Exception {
    BasicTypes.EncodedValue.Builder builder = BasicTypes.EncodedValue.newBuilder();
    BasicTypes.EncodedValue encodedValue = builder.setIntResult(1).build();
    assertEquals(1, protobufSerializationService.decode(encodedValue));
  }

  @Test
  public void getLongPrimitiveFromEncodedValue() throws Exception {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setLongResult(1).build();
    assertEquals(1l, protobufSerializationService.decode(encodedValue));
  }

  @Test
  public void getShortPrimitiveFromEncodedValue() throws Exception {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setShortResult(1).build();
    assertEquals((short) 1, protobufSerializationService.decode(encodedValue));
  }

  @Test
  public void getBytePrimitiveFromEncodedValue() throws Exception {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setByteResult(1).build();
    assertEquals((byte) 1, protobufSerializationService.decode(encodedValue));
  }

  @Test
  public void getBooleanPrimitiveFromEncodedValue() throws Exception {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setBooleanResult(true).build();
    assertEquals(true, protobufSerializationService.decode(encodedValue));
  }

  @Test
  public void getDoublePrimitiveFromEncodedValue() throws Exception {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setDoubleResult(1.0).build();
    assertEquals(1.0, protobufSerializationService.decode(encodedValue));
  }

  @Test
  public void getFloatPrimitiveFromEncodedValue() throws Exception {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setFloatResult(1).build();
    assertEquals(1f, protobufSerializationService.decode(encodedValue));
  }

  @Test
  public void getByteArrayPrimitiveFromEncodedValue() throws Exception {
    BasicTypes.EncodedValue encodedValue = BasicTypes.EncodedValue.newBuilder()
        .setBinaryResult(ByteString.copyFrom("SomeBinary".getBytes())).build();
    assertArrayEquals("SomeBinary".getBytes(Charset.forName("UTF-8")),
        (byte[]) protobufSerializationService.decode(encodedValue));
  }

  @Test
  public void getStringPrimitiveFromEncodedValue() throws Exception {
    BasicTypes.EncodedValue encodedValue =
        BasicTypes.EncodedValue.newBuilder().setStringResult("SomeString").build();
    assertEquals("SomeString", protobufSerializationService.decode(encodedValue));
  }

  @Test
  public void doesAIntValueSuccessfullyEncodeIntoPrimitiveEncodedValues() throws Exception {
    createAndVerifyEncodedValue(1);
  }

  @Test
  public void doesALongValueSuccessfullyEncodeIntoPrimitiveEncodedValues() throws Exception {
    createAndVerifyEncodedValue(1l);
  }

  @Test
  public void doesAShortValueSuccessfullyEncodeIntoPrimitiveEncodedValues() throws Exception {
    createAndVerifyEncodedValue((short) 1);
  }

  @Test
  public void doesAByteValueSuccessfullyEncodeIntoPrimitiveEncodedValues() throws Exception {
    createAndVerifyEncodedValue((byte) 1);
  }

  @Test
  public void doesABooleanValueSuccessfullyEncodeIntoPrimitiveEncodedValues() throws Exception {
    createAndVerifyEncodedValue(true);
  }

  @Test
  public void doesADoubleValueSuccessfullyEncodeIntoPrimitiveEncodedValues() throws Exception {
    createAndVerifyEncodedValue((double) 1);
  }

  @Test
  public void doesAFloatValueSuccessfullyEncodeIntoPrimitiveEncodedValues() throws Exception {
    createAndVerifyEncodedValue((float) 1);
  }

  @Test
  public void doesABinaryValueSuccessfullyEncodeIntoPrimitiveEncodedValues() throws Exception {
    createAndVerifyEncodedValue("Some Text to Binary".getBytes());
  }

  @Test
  public void doesAStringValueSuccessfullyEncodeIntoPrimitiveEncodedValues() throws Exception {
    createAndVerifyEncodedValue("Some String text to test");
  }

  @Test
  public void doesANullValueSuccessfullyEncodeIntoEncodedValues() throws Exception {
    createAndVerifyEncodedValue(null);
  }

  private <T> void createAndVerifyEncodedValue(T testObj) throws EncodingException {
    BasicTypes.EncodedValue encodedValue = protobufSerializationService.encode(testObj);
    if (testObj instanceof byte[]) {
      try {
        assertArrayEquals((byte[]) testObj,
            (byte[]) protobufSerializationService.decode(encodedValue));
      } catch (org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException e) {
        e.printStackTrace();
      }
    } else {
      try {
        assertEquals(testObj, protobufSerializationService.decode(encodedValue));
      } catch (org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException e) {
        e.printStackTrace();
      }
    }
  }
}
