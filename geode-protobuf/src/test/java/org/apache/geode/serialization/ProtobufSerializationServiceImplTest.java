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
package org.apache.geode.serialization;

import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ProtobufSerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(UnitTest.class)
public class ProtobufSerializationServiceImplTest {

  public static final String PAYLOAD = "my value";

  @Test
  public void valuesPreservedByEncodingThenDecoding() throws CodecAlreadyRegisteredForTypeException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    ProtobufSerializationService protobufSerializationService = new ProtobufSerializationService();

    testEncodeDecode(protobufSerializationService, BasicTypes.EncodingType.STRING, "testString");
    testEncodeDecode(protobufSerializationService, BasicTypes.EncodingType.FLOAT, (float) 34.23);
    testEncodeDecode(protobufSerializationService, BasicTypes.EncodingType.DOUBLE, 34.23);
    testEncodeDecode(protobufSerializationService, BasicTypes.EncodingType.INT, 45);
    testEncodeDecode(protobufSerializationService, BasicTypes.EncodingType.SHORT, (short) 45);
    testEncodeDecode(protobufSerializationService, BasicTypes.EncodingType.BYTE, (byte) 45);
    testEncodeDecode(protobufSerializationService, BasicTypes.EncodingType.LONG, (long) 45);
    testEncodeDecode(protobufSerializationService, BasicTypes.EncodingType.BOOLEAN, false);
    testEncodeDecode(protobufSerializationService, BasicTypes.EncodingType.BOOLEAN, true);
    testEncodeDecode(protobufSerializationService, BasicTypes.EncodingType.BINARY,
        "testString".getBytes());
    // TODO: Test JSON conversion
  }

  private void testEncodeDecode(ProtobufSerializationService service,
      BasicTypes.EncodingType encodingType, Object data)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    byte[] encodedValue = service.encode(encodingType, data);
    Object decodedValue = service.decode(encodingType, encodedValue);
    Assert.assertEquals(data, decodedValue);
  }
}
