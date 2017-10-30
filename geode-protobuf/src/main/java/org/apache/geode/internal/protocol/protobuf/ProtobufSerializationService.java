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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.serialization.SerializationType;
import org.apache.geode.internal.protocol.serialization.TypeCodec;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.internal.protocol.serialization.registry.SerializationCodecRegistry;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;

@Experimental
public class ProtobufSerializationService implements SerializationService<BasicTypes.EncodingType> {
  private SerializationCodecRegistry serializationCodecRegistry = new SerializationCodecRegistry();

  public ProtobufSerializationService() {}

  @Override
  public byte[] encode(BasicTypes.EncodingType encodingTypeValue, Object value)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    TypeCodec codecForType = getTypeCodecForProtobufType(encodingTypeValue);
    return codecForType.encode(value);
  }

  @Override
  public Object decode(BasicTypes.EncodingType encodingTypeValue, byte[] value)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    if (encodingTypeValue == BasicTypes.EncodingType.INVALID) {
      return null;
    }
    TypeCodec codecForType = getTypeCodecForProtobufType(encodingTypeValue);
    return codecForType.decode(value);
  }

  private TypeCodec getTypeCodecForProtobufType(BasicTypes.EncodingType encodingTypeValue)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    SerializationType serializationTypeForEncodingType =
        EncodingTypeTranslator.getSerializationTypeForEncodingType(encodingTypeValue);

    return serializationCodecRegistry.getCodecForType(serializationTypeForEncodingType);
  }
}
