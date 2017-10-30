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
package org.apache.geode.internal.protocol.protobuf.utilities;

import com.google.protobuf.ByteString;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.internal.protocol.protobuf.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.EncodingTypeTranslator;
import org.apache.geode.internal.protocol.protobuf.ProtobufSerializationService;
import org.apache.geode.internal.protocol.protobuf.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.utilities.exception.UnknownProtobufPrimitiveType;
import org.apache.geode.internal.protocol.serialization.SerializationService;
import org.apache.geode.internal.protocol.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.internal.protocol.serialization.registry.exception.CodecNotRegisteredForTypeException;

/**
 * This class contains helper functions for assistance in creating protobuf objects. This class is
 * mainly focused on helper functions which can be used in building BasicTypes for use in other
 * messages or those used to create the top level Message objects.
 * <p>
 * Helper functions specific to creating ClientProtocol.Responses can be found at
 * {@link ProtobufResponseUtilities} Helper functions specific to creating ClientProtocol.Requests
 * can be found at {@link ProtobufRequestUtilities}
 */
@Experimental
public abstract class ProtobufUtilities {
  /**
   * Creates a object containing the type and value encoding of a piece of data
   *
   * @param serializationService - object which knows how to encode objects for the protobuf
   *        protocol {@link ProtobufSerializationService}
   * @param unencodedValue - the value object which is to be encoded
   * @return a protobuf EncodedValue object
   * @throws UnsupportedEncodingTypeException - The object passed doesn't have a corresponding
   *         SerializationType
   * @throws CodecNotRegisteredForTypeException - There isn't a protobuf codec for the
   *         SerializationType of the passed object
   */
  public static BasicTypes.EncodedValue createEncodedValue(
      SerializationService serializationService, Object unencodedValue)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {

    try {
      return createPrimitiveEncodedValue(unencodedValue);
    } catch (UnknownProtobufPrimitiveType e) {
      BasicTypes.EncodingType resultEncodingType =
          EncodingTypeTranslator.getEncodingTypeForObject(unencodedValue);
      byte[] encodedValue = serializationService.encode(resultEncodingType, unencodedValue);
      BasicTypes.CustomEncodedValue.Builder customEncodedValueBuilder =
          BasicTypes.CustomEncodedValue.newBuilder().setEncodingType(resultEncodingType)
              .setValue(ByteString.copyFrom(encodedValue));
      return BasicTypes.EncodedValue.newBuilder().setCustomEncodedValue(customEncodedValueBuilder)
          .build();
    }
  }

  /**
   * Creates a protobuf key,value pair from an encoded key and value
   *
   * @param key - an EncodedValue containing the key of the entry
   * @param value - an EncodedValue containing the value of the entry
   * @return a protobuf Entry object containing the passed key and value
   */
  public static BasicTypes.Entry createEntry(BasicTypes.EncodedValue key,
      BasicTypes.EncodedValue value) {
    return BasicTypes.Entry.newBuilder().setKey(key).setValue(value).build();
  }

  /**
   * Creates a protobuf (key, value) pair from unencoded data. if the value is null, it will be
   * unset in the BasicTypes.Entry.
   *
   * The key MUST NOT be null.
   *
   * @param serializationService - object which knows how to encode objects for the protobuf
   *        protocol {@link ProtobufSerializationService}
   * @param unencodedKey - the unencoded key for the entry
   * @param unencodedValue - the unencoded value for the entry
   * @return a protobuf Entry containing the encoded key and value
   * @throws UnsupportedEncodingTypeException - The key or value passed doesn't have a corresponding
   *         SerializationType
   * @throws CodecNotRegisteredForTypeException - There isn't a protobuf codec for the
   *         SerializationType of the passed key or value
   */
  public static BasicTypes.Entry createEntry(SerializationService serializationService,
      Object unencodedKey, Object unencodedValue)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {
    if (unencodedValue == null) {
      return BasicTypes.Entry.newBuilder()
          .setKey(createEncodedValue(serializationService, unencodedKey)).build();
    }
    return createEntry(createEncodedValue(serializationService, unencodedKey),
        createEncodedValue(serializationService, unencodedValue));
  }

  /**
   * This creates a protobuf message containing a ClientProtocol.Response
   *
   * @param response - The response for the message
   * @return a protobuf Message containing the above parameters
   */
  public static ClientProtocol.Message createProtobufResponse(ClientProtocol.Response response) {
    return ClientProtocol.Message.newBuilder().setResponse(response).build();
  }

  /**
   * This creates a protobuf message containing a ClientProtocol.Request
   *
   * @param request - The request for the message
   * @return a protobuf Message containing the above parameters
   */
  public static ClientProtocol.Message createProtobufMessage(ClientProtocol.Request request) {
    return ClientProtocol.Message.newBuilder().setRequest(request).build();
  }

  /**
   * This creates a protobuf message containing a ClientProtocol.Request
   *
   * @param getAllRequest - The request for the message
   * @return a protobuf Message containing the above parameters
   */
  public static ClientProtocol.Request createProtobufRequestWithGetAllRequest(
      RegionAPI.GetAllRequest getAllRequest) {
    return ClientProtocol.Request.newBuilder().setGetAllRequest(getAllRequest).build();
  }

  /**
   * This will return the object encoded in a protobuf EncodedValue
   *
   * @param serializationService - object which knows how to encode objects for the protobuf
   *        protocol {@link ProtobufSerializationService}
   * @param encodedValue - The value to be decoded
   * @return the object encoded in the passed encodedValue
   * @throws UnsupportedEncodingTypeException - There isn't a SerializationType matching the
   *         encodedValues type
   * @throws CodecNotRegisteredForTypeException - There isn't a protobuf codec for the
   *         SerializationType matching the encodedValues type
   */
  public static Object decodeValue(SerializationService serializationService,
      BasicTypes.EncodedValue encodedValue)
      throws UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException {

    if (encodedValue.getValueCase() == BasicTypes.EncodedValue.ValueCase.CUSTOMENCODEDVALUE) {
      BasicTypes.CustomEncodedValue customEncodedValue = encodedValue.getCustomEncodedValue();
      return serializationService.decode(customEncodedValue.getEncodingType(),
          customEncodedValue.getValue().toByteArray());
    } else {
      try {
        return getPrimitiveValueFromEncodedValue(encodedValue);
      } catch (UnknownProtobufPrimitiveType unknownProtobufPrimitiveType) {
        throw new UnsupportedEncodingTypeException("Unknown primitive type encoding",
            unknownProtobufPrimitiveType);
      }
    }
  }

  /**
   * @return a Protobuf BasicTypes.Region message that represents the {@link Region}
   */

  public static BasicTypes.Region createRegionMessageFromRegion(Region region) {
    RegionAttributes regionAttributes = region.getAttributes();
    BasicTypes.Region.Builder protoRegionBuilder = BasicTypes.Region.newBuilder();

    protoRegionBuilder.setName(region.getName());
    protoRegionBuilder.setSize(region.size());

    protoRegionBuilder.setPersisted(regionAttributes.getDataPolicy().withPersistence());
    if (regionAttributes.getKeyConstraint() != null) {
      protoRegionBuilder.setKeyConstraint(regionAttributes.getKeyConstraint().toString());
    }
    if (regionAttributes.getValueConstraint() != null) {
      protoRegionBuilder.setValueConstraint(regionAttributes.getValueConstraint().toString());
    }

    protoRegionBuilder.setScope(regionAttributes.getScope().toString());
    protoRegionBuilder.setDataPolicy(regionAttributes.getDataPolicy().toString());
    return protoRegionBuilder.build();
  }

  public static ClientProtocol.Request.Builder createProtobufRequestBuilder() {
    return ClientProtocol.Request.newBuilder();
  }

  /**
   * This will create an EncodedValue message for a primitive type.
   *
   * @param valueToEncode this represents the potential primitive value that needs to be encoded in
   *        an EncodedValue
   * @return EncodedValue message with the correct primitive value populated
   * @throws UnknownProtobufPrimitiveType
   */
  static BasicTypes.EncodedValue createPrimitiveEncodedValue(Object valueToEncode)
      throws UnknownProtobufPrimitiveType {
    ProtobufPrimitiveTypes protobufPrimitiveTypes =
        ProtobufPrimitiveTypes.valueOf(valueToEncode.getClass());
    BasicTypes.EncodedValue.Builder builder = BasicTypes.EncodedValue.newBuilder();
    switch (protobufPrimitiveTypes) {
      case INT: {
        builder.setIntResult((Integer) valueToEncode);
        break;
      }
      case LONG: {
        builder.setLongResult((Long) valueToEncode);
        break;
      }
      case SHORT: {
        builder.setShortResult((Short) valueToEncode);
        break;
      }
      case BYTE: {
        builder.setByteResult((Byte) valueToEncode);
        break;
      }
      case DOUBLE: {
        builder.setDoubleResult((Double) valueToEncode);
        break;
      }
      case FLOAT: {
        builder.setFloatResult((Float) valueToEncode);
        break;
      }
      case BINARY: {
        builder.setBinaryResult(ByteString.copyFrom((byte[]) valueToEncode));
        break;
      }
      case BOOLEAN: {
        builder.setBooleanResult((Boolean) valueToEncode);
        break;
      }
      case STRING: {
        builder.setStringResult((String) valueToEncode);
        break;
      }

    }
    return builder.build();
  }

  static Object getPrimitiveValueFromEncodedValue(BasicTypes.EncodedValue encodedValue)
      throws UnknownProtobufPrimitiveType {
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
      default:
        throw new UnknownProtobufPrimitiveType(
            "Unknown primitive type for: " + encodedValue.getValueCase());
    }
  }
}
