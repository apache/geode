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
package org.apache.geode.internal.protocol.protobuf.v1;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.protobuf.ByteString;
import com.google.protobuf.NullValue;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.JsonPdxConverter;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.SerializationService;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.DecodingException;
import org.apache.geode.internal.protocol.protobuf.v1.serialization.exception.EncodingException;
import org.apache.geode.internal.protocol.protobuf.v1.utilities.exception.UnknownProtobufEncodingType;
import org.apache.geode.internal.protocol.serialization.NoOpCustomValueSerializer;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.protocol.serialization.ValueSerializer;

@Experimental
public class ProtobufSerializationService implements SerializationService<BasicTypes.EncodedValue> {
  private final JsonPdxConverter jsonPdxConverter = new JsonPdxConverter();
  private final ValueSerializer serializer;

  public ProtobufSerializationService() {
    this(new NoOpCustomValueSerializer());
  }

  public ProtobufSerializationService(ValueSerializer valueSerializer) {
    this.serializer = valueSerializer;
  }

  /**
   * @param value the value to be encoded
   *
   * @return EncodedValue message with the serialized value
   */
  @Override
  public BasicTypes.EncodedValue encode(Object value) throws EncodingException {
    BasicTypes.EncodedValue.Builder builder = BasicTypes.EncodedValue.newBuilder();
    try {
      if (serializer.supportsPrimitives()) {
        ByteString encoded = serializer.serialize(value);
        return builder.setCustomObjectResult(encoded).build();
      }

      if (value == null) {
        return builder.setNullResult(NullValue.NULL_VALUE).build();
      }

      ProtobufEncodingTypes protobufEncodingTypes = ProtobufEncodingTypes.valueOf(value.getClass());
      switch (protobufEncodingTypes) {
        case INT: {
          builder.setIntResult((Integer) value);
          break;
        }
        case LONG: {
          builder.setLongResult((Long) value);
          break;
        }
        case SHORT: {
          builder.setShortResult((Short) value);
          break;
        }
        case BYTE: {
          builder.setByteResult((Byte) value);
          break;
        }
        case DOUBLE: {
          builder.setDoubleResult((Double) value);
          break;
        }
        case FLOAT: {
          builder.setFloatResult((Float) value);
          break;
        }
        case BINARY: {
          builder.setBinaryResult(ByteString.copyFrom((byte[]) value));
          break;
        }
        case BOOLEAN: {
          builder.setBooleanResult((Boolean) value);
          break;
        }
        case STRING: {
          builder.setStringResult((String) value);
          break;
        }
        default: {
          ByteString customResult = customSerialize(value);
          if (customResult != null) {
            builder.setCustomObjectResult(customResult);
          } else if (value instanceof PdxInstance) {
            builder.setJsonObjectResult(jsonPdxConverter.encode((PdxInstance) value));
          } else {
            throw new EncodingException("No handler for object type " + value.getClass());
          }
          break;
        }
      }
    } catch (UnknownProtobufEncodingType e) {
      throw new EncodingException("No protobuf encoding for type " + value.getClass().getName(), e);
    } catch (IOException e) {
      throw new EncodingException("Error encoding type " + value.getClass().getName(), e);
    }

    return builder.build();
  }

  private ByteString customSerialize(Object value) throws IOException {
    return serializer instanceof NoOpCustomValueSerializer ? null : serializer.serialize(value);
  }

  /**
   * @param encodedValue - The value to be decoded
   * @return A decoded object representing encodedValue
   * @throws EncodingException if the value cannot be decoded.
   */
  @Override
  public Object decode(BasicTypes.EncodedValue encodedValue) throws DecodingException {

    try {
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
          return jsonPdxConverter.decode(encodedValue.getJsonObjectResult());
        case NULLRESULT:
          return null;
        case CUSTOMOBJECTRESULT:
          return serializer.deserialize(encodedValue.getCustomObjectResult());
        default:
          throw new DecodingException(
              "Unknown Protobuf encoding type: " + encodedValue.getValueCase());
      }
    } catch (IOException | ClassNotFoundException e) {
      throw new DecodingException("Error decoding value", e);
    }
  }

  public Collection<Object> decodeList(Collection<BasicTypes.EncodedValue> encodedValues) {
    return encodedValues.stream().map(this::decode).collect(Collectors.toList());
  }

  /**
   * Maps classes to encoding for protobuf.
   *
   * This currently conflates object type with serialization, which may be an issue if we add more
   * types of object serialization.
   */
  private enum ProtobufEncodingTypes {

    // If any classes are added to this list that are not final, the logic
    // in valueOf must change. It currently works only if the user's class
    // is exactly the same as the class listed here.
    STRING(String.class),
    INT(Integer.class),
    LONG(Long.class),
    SHORT(Short.class),
    BYTE(Byte.class),
    BOOLEAN(Boolean.class),
    DOUBLE(Double.class),
    FLOAT(Float.class),
    BINARY(byte[].class),
    OTHER(Object.class);

    private Class clazz;

    private static Map<Class, ProtobufEncodingTypes> classToType = new HashMap<>();

    static {
      for (ProtobufEncodingTypes type : values()) {
        classToType.put(type.clazz, type);
      }
    }

    ProtobufEncodingTypes(Class clazz) {
      this.clazz = clazz;
    }

    public static ProtobufEncodingTypes valueOf(Class unencodedValueClass)
        throws UnknownProtobufEncodingType {
      ProtobufEncodingTypes type = classToType.get(unencodedValueClass);
      if (type != null) {
        return type;
      } else {
        return OTHER;
      }
    }
  }
}
