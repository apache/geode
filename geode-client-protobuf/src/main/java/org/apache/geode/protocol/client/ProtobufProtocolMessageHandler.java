/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.protocol.client;

import static org.apache.geode.protocol.client.EncodingTypeThingyKt.getEncodingTypeForObjectKT;
import static org.apache.geode.protocol.client.EncodingTypeThingyKt.serializerFromProtoEnum;
import static org.apache.geode.protocol.protobuf.ClientProtocol.Message;
import static org.apache.geode.protocol.protobuf.ClientProtocol.Request;
import static org.apache.geode.protocol.protobuf.ClientProtocol.Response;
import static org.apache.geode.protocol.protobuf.RegionAPI.PutRequest;

import com.google.protobuf.ByteString;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheWriterException;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolMessageHandler;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.RegionAPI;
import org.apache.geode.protocol.protobuf.RegionAPI.GetRequest;
import org.apache.geode.protocol.protobuf.RegionAPI.PutResponse;
import org.apache.geode.serialization.SerializationType;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ProtobufProtocolMessageHandler implements ClientProtocolMessageHandler {

  private static final Logger logger = LogService.getLogger();

  private String ErrorMessageFromMessage(Message message) {
    return "Error parsing message, message string: " + message.toString();
  }

  @Override
  public void receiveMessage(InputStream inputStream, OutputStream outputStream, Cache cache)
      throws IOException {
    final Message message = Message.parseDelimitedFrom(inputStream);
    // can be null at EOF, see Parser.parseDelimitedFrom(java.io.InputStream)
    if (message == null) {
      return;
    }

    if (message.getMessageTypeCase() != Message.MessageTypeCase.REQUEST) {
      // TODO
      logger.error(() -> "Got message of type response: " + ErrorMessageFromMessage(message));
    }

    Message responseMessage = null;

    Request request = message.getRequest();
    Request.RequestAPICase requestAPICase = request.getRequestAPICase();
    if (requestAPICase == Request.RequestAPICase.GETREQUEST) {
      responseMessage = doGetRequest(request.getGetRequest(), cache);
    } else if (requestAPICase == Request.RequestAPICase.PUTREQUEST) {
      responseMessage = doPutRequest(request.getPutRequest(), cache);
    } else {
      // TODO
    }
    if (responseMessage != null) {
      responseMessage.writeDelimitedTo(outputStream);
    }
  }

  private Message doPutRequest(PutRequest request, Cache cache) {
    final String regionName = request.getRegionName();
    final BasicTypes.Entry entry = request.getEntry();
    assert (entry != null);

    final Region<Object, Object> region = cache.getRegion(regionName);
    try {
      region.put(deserializeEncodedValue(entry.getKey()),
          deserializeEncodedValue(entry.getValue()));
      return putResponseWithStatus(true);
    } catch (TimeoutException | CacheWriterException ex) {
      logger.warn("Caught normal-ish exception doing region put", ex);
      return putResponseWithStatus(false);
    }
  }

  private Object deserializeEncodedValue(BasicTypes.EncodedValue encodedValue) {
    assert (encodedValue != null);
    SerializationType serializer = serializerFromProtoEnum(encodedValue.getEncodingType());
    return serializer.deserialize(encodedValue.getValue().toByteArray());
  }

  private byte[] serializeEncodedValue(BasicTypes.EncodingType encodingType,
      Object objectToSerialize) {
    if (objectToSerialize == null) {
      return null; // BLECH!!! :(
    }
    SerializationType serializer = serializerFromProtoEnum(encodingType);
    return serializer.serialize(objectToSerialize);
  }

  private Message putResponseWithStatus(boolean ok) {
    return Message.newBuilder()
        .setResponse(Response.newBuilder().setPutResponse(PutResponse.newBuilder().setSuccess(ok)))
        .build();
  }

  private Message doGetRequest(GetRequest request, Cache cache) {
    Region<Object, Object> region = cache.getRegion(request.getRegionName());
    Object returnValue = region.get(deserializeEncodedValue(request.getKey()));

    if (returnValue == null) {

      return makeGetResponseMessageWithValue(new byte[0]);
    } else {
      // TODO types in the region?
      return makeGetResponseMessageWithValue(returnValue);
    }
  }

  private BasicTypes.EncodingType getEncodingTypeForObject(Object object) {
    return getEncodingTypeForObjectKT(object);
  }

  private Message makeGetResponseMessageWithValue(Object objectToReturn) {
    BasicTypes.EncodingType encodingType = getEncodingTypeForObject(objectToReturn);
    byte[] serializedObject = serializeEncodedValue(encodingType, objectToReturn);
    return Message.newBuilder()
        .setResponse(Response.newBuilder()
            .setGetResponse(RegionAPI.GetResponse.newBuilder()
                .setResult(BasicTypes.EncodedValue.newBuilder()
                    .setEncodingType(BasicTypes.EncodingType.STRING)
                    .setValue(ByteString.copyFrom(serializedObject)))))
        .build();
  }
}
