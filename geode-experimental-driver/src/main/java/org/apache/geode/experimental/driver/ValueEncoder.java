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

import java.io.IOException;
import java.util.Objects;

import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;

/**
 * Encodes and decodes Java objects to and from Protobuf encoded values.
 *
 * <strong>This code is an experimental prototype and is presented "as is" with no warranty,
 * suitability, or fitness of purpose implied.</strong>
 */
@Experimental
class ValueEncoder {

  private final ValueSerializer valueSerializer;

  public ValueEncoder(ValueSerializer valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  /**
   * Encodes a Java object into a Protobuf encoded value.
   *
   * @param unencodedValue Java object to encode.
   * @return Encoded value of the Java object.
   */
  BasicTypes.EncodedValue encodeValue(Object unencodedValue) {
    BasicTypes.EncodedValue.Builder builder = BasicTypes.EncodedValue.newBuilder();

    if (valueSerializer.supportsPrimitives()) {
      ByteString customBytes = customSerialize(unencodedValue);
      return builder.setCustomObjectResult(customBytes).build();
    }

    if (Objects.isNull(unencodedValue)) {
      builder.setNullResult(NullValue.NULL_VALUE);
    } else if (Integer.class.equals(unencodedValue.getClass())) {
      builder.setIntResult((Integer) unencodedValue);
    } else if (Long.class.equals(unencodedValue.getClass())) {
      builder.setLongResult((Long) unencodedValue);
    } else if (Short.class.equals(unencodedValue.getClass())) {
      builder.setShortResult((Short) unencodedValue);
    } else if (Byte.class.equals(unencodedValue.getClass())) {
      builder.setByteResult((Byte) unencodedValue);
    } else if (Double.class.equals(unencodedValue.getClass())) {
      builder.setDoubleResult((Double) unencodedValue);
    } else if (Float.class.equals(unencodedValue.getClass())) {
      builder.setFloatResult((Float) unencodedValue);
    } else if (byte[].class.equals(unencodedValue.getClass())) {
      builder.setBinaryResult(ByteString.copyFrom((byte[]) unencodedValue));
    } else if (Boolean.class.equals(unencodedValue.getClass())) {
      builder.setBooleanResult((Boolean) unencodedValue);
    } else if (String.class.equals(unencodedValue.getClass())) {
      builder.setStringResult((String) unencodedValue);
    } else if (JSONWrapper.class.isAssignableFrom(unencodedValue.getClass())) {
      builder.setJsonObjectResult(((JSONWrapper) unencodedValue).getJSON());
    } else {
      ByteString customBytes = customSerialize(unencodedValue);
      if (customBytes != null) {
        builder.setCustomObjectResult(customBytes);
      } else {
        throw new IllegalStateException("We don't know how to handle an object of type "
            + unencodedValue.getClass() + ": " + unencodedValue);
      }
    }

    return builder.build();
  }

  private ByteString customSerialize(Object unencodedValue) {
    try {
      return valueSerializer.serialize(unencodedValue);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Decodes a Protobuf encoded value into a Java object.
   *
   * @param encodedValue Encoded value to decode.
   * @return Decoded Java object.
   */
  @SuppressWarnings("unchecked")
  <T> T decodeValue(BasicTypes.EncodedValue encodedValue) {
    return (T) decodeValueObject(encodedValue);
  }

  private Object decodeValueObject(BasicTypes.EncodedValue encodedValue) {
    switch (encodedValue.getValueCase()) {
      case BINARYRESULT:
        return encodedValue.getBinaryResult().toByteArray();
      case BOOLEANRESULT:
        return encodedValue.getBooleanResult();
      case BYTERESULT:
        return (byte) encodedValue.getByteResult();
      case DOUBLERESULT:
        return encodedValue.getDoubleResult();
      case FLOATRESULT:
        return encodedValue.getFloatResult();
      case INTRESULT:
        return encodedValue.getIntResult();
      case LONGRESULT:
        return encodedValue.getLongResult();
      case SHORTRESULT:
        return (short) encodedValue.getShortResult();
      case STRINGRESULT:
        return encodedValue.getStringResult();
      case JSONOBJECTRESULT:
        return JSONWrapper.wrapJSON(encodedValue.getJsonObjectResult());
      case NULLRESULT:
        return null;
      case CUSTOMOBJECTRESULT:
        try {
          return valueSerializer.deserialize(encodedValue.getCustomObjectResult());
        } catch (IOException | ClassNotFoundException e) {
          throw new IllegalStateException(e);
        }
      default:
        throw new IllegalStateException(
            "Can't decode a value of type " + encodedValue.getValueCase() + ": " + encodedValue);
    }
  }

  /**
   * Encodes a Java object key and a Java object value into a Protobuf encoded entry.
   *
   * @param unencodedKey Java object key to encode.
   * @param unencodedValue Java object value to encode.
   * @return Encoded entry of the Java object key and value.
   */
  BasicTypes.Entry encodeEntry(Object unencodedKey, Object unencodedValue) {
    if (unencodedValue == null) {
      return BasicTypes.Entry.newBuilder().setKey(encodeValue(unencodedKey)).build();
    }
    return BasicTypes.Entry.newBuilder().setKey(encodeValue(unencodedKey))
        .setValue(encodeValue(unencodedValue)).build();
  }
}
