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
package org.apache.geode.experimental.driver;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.io.UnsupportedEncodingException;

import com.google.protobuf.ByteString;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.test.junit.categories.UnitTest;

@Category(UnitTest.class)
public class ValueEncoderTest {
  @Test
  public void encodeAndDecode() throws Exception {
    final Object[] objects = {37, (short) 37, (byte) 37, 37L, 37., 37.F, true, "hello, world"};
    for (Object object : objects) {
      assertEquals(object, ValueEncoder.decodeValue(ValueEncoder.encodeValue(object)));
    }

    final byte[] bytes = new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
    assertArrayEquals(bytes, (byte[]) ValueEncoder.decodeValue(ValueEncoder.encodeValue(bytes)));
  }

  @Test(expected = IllegalStateException.class)
  public void cantDecodeJson() throws UnsupportedEncodingException {
    BasicTypes.EncodedValue.Builder builder = BasicTypes.EncodedValue.newBuilder();
    BasicTypes.CustomEncodedValue.Builder customEncodedValue =
        BasicTypes.CustomEncodedValue.newBuilder().setValue(ByteString.copyFrom("hello", "UTF-8"));
    builder.setCustomEncodedValue(customEncodedValue);
    ValueEncoder.decodeValue(builder.build());
  }
}
