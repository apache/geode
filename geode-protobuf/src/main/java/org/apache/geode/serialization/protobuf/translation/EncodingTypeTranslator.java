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
package org.apache.geode.serialization.protobuf.translation;

import org.apache.geode.pdx.JSONFormatter;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.serialization.SerializationType;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;

public abstract class EncodingTypeTranslator {
  public static SerializationType getSerializationTypeForEncodingType(
      BasicTypes.EncodingType encodingType) throws UnsupportedEncodingTypeException {
    switch (encodingType) {
      case INT:
        return SerializationType.INT;
      case BYTE:
        return SerializationType.BYTE;
      case JSON:
        return SerializationType.JSON;
      case LONG:
        return SerializationType.LONG;
      case FLOAT:
        return SerializationType.FLOAT;
      case SHORT:
        return SerializationType.SHORT;
      case BINARY:
        return SerializationType.BINARY;
      case DOUBLE:
        return SerializationType.DOUBLE;
      case STRING:
        return SerializationType.STRING;
      case BOOLEAN:
        return SerializationType.BOOLEAN;
      default:
        throw new UnsupportedEncodingTypeException(
            "No serialization type found for protobuf encoding type: " + encodingType);
    }
  }

  public static BasicTypes.EncodingType getEncodingTypeForObject(Object resultValue)
      throws UnsupportedEncodingTypeException {
    if (resultValue instanceof Integer) {
      return BasicTypes.EncodingType.INT;
    } else if (resultValue instanceof Byte) {
      return BasicTypes.EncodingType.BYTE;
    } else if (resultValue instanceof PdxInstance) {
      String pdxClassName = ((PdxInstance) resultValue).getClassName();
      if (pdxClassName.equals(JSONFormatter.JSON_CLASSNAME)) {
        return BasicTypes.EncodingType.JSON;
      }
    } else if (resultValue instanceof Long) {
      return BasicTypes.EncodingType.LONG;
    } else if (resultValue instanceof Float) {
      return BasicTypes.EncodingType.FLOAT;
    } else if (resultValue instanceof Short) {
      return BasicTypes.EncodingType.SHORT;
    } else if (resultValue instanceof byte[]) {
      return BasicTypes.EncodingType.BINARY;
    } else if (resultValue instanceof Double) {
      return BasicTypes.EncodingType.DOUBLE;
    } else if (resultValue instanceof String) {
      return BasicTypes.EncodingType.STRING;
    } else if (resultValue instanceof Boolean) {
      return BasicTypes.EncodingType.BOOLEAN;
    }

    throw new UnsupportedEncodingTypeException(
        "We cannot translate: " + resultValue.getClass() + " into a specific Protobuf Encoding");
  }
}
