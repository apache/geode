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
import org.apache.geode.serialization.Deserializer;
import org.apache.geode.serialization.Serializer;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.apache.geode.protocol.protobuf.ClientProtocol.Message;
import static org.apache.geode.protocol.protobuf.ClientProtocol.Request;
import static org.apache.geode.protocol.protobuf.ClientProtocol.Response;
import static org.apache.geode.protocol.protobuf.RegionAPI.PutRequest;

public class ProtobufProtocolMessageHandler implements ClientProtocolMessageHandler {

  private static final Logger logger = LogService.getLogger();

  private String ErrorMessageFromMessage(Message message) {
    return "Error parsing message, message string: " + message.toString();
  }

  @Override
  public void receiveMessage(InputStream inputStream, OutputStream outputStream,
                             Deserializer deserializer, Serializer serializer, Cache cache) throws IOException {
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
      responseMessage = doGetRequest(request.getGetRequest(), deserializer, serializer, cache);
    } else if (requestAPICase == Request.RequestAPICase.PUTREQUEST) {
      responseMessage = doPutRequest(request.getPutRequest(), deserializer, cache);
    } else {
      // TODO
    }
    if (responseMessage != null) {
    responseMessage.writeDelimitedTo(outputStream);
    }
  }

  private Message doPutRequest(PutRequest request, Deserializer dataDeserializer, Cache cache) {
    final String regionName = request.getRegionName();
    final BasicTypes.Entry entry = request.getEntry();
    final ByteString key = entry.getKey().getKey();
    final ByteString value = entry.getValue().getValue();

    final Region<Object, Object> region = cache.getRegion(regionName);
    try {
      region.put(dataDeserializer.deserialize(key.toByteArray()),
          dataDeserializer.deserialize(value.toByteArray()));
      return putResponseWithStatus(true);
    } catch (TimeoutException | CacheWriterException ex) {
      logger.warn("Caught normal-ish exception doing region put", ex);
      return putResponseWithStatus(false);
    }
  }

  private Message putResponseWithStatus(boolean ok) {
    return Message.newBuilder()
        .setResponse(Response.newBuilder().setPutResponse(PutResponse.newBuilder().setSuccess(ok)))
        .build();
  }

  private Message doGetRequest(GetRequest request, Deserializer deserializer, Serializer serializer, Cache cache) {
    String regionName = request.getRegionName();
    BasicTypes.Key key = request.getKey();
    byte[] keyBytes = key.getKey().toByteArray();
    Region<Object, Object> region = cache.getRegion(regionName);

    Object returnValue = region.get(deserializer.deserialize(keyBytes));

    if (returnValue == null) {
      return getResponseWithValue(new byte[0]);
    } else {
      // TODO types in the region?
      return getResponseWithValue(serializer.serialize(returnValue));
    }
  }

  private Message getResponseWithValue(byte[] value) {
    return Message.newBuilder()
      .setResponse(Response.newBuilder().setGetResponse(RegionAPI.GetResponse.newBuilder()
        .setResult(BasicTypes.Value.newBuilder().setValue(ByteString.copyFrom(value)))))
      .build();
  }

  public ProtobufProtocolMessageHandler() {}
}


// public final class NewClientProtocol {
// public static void recvMessage(Cache cache, InputStream inputStream, OutputStream outputStream) {
// try {
// final DataInputStream dataInputStream = new DataInputStream(inputStream);
// final DataOutputStream dataOutputStream = new DataOutputStream(outputStream);
//
// RequestHeader header = new RequestHeader(dataInputStream);
//
// // todo: string -- assume UTF-8 from here.
// // Java modified UTF-8: unsigned short len. hope we don't run into the "modified" part.
// switch (header.requestType) {
// case MessageType.PUT:
// servePutRequest(header, cache, dataInputStream, dataOutputStream);
// break;
// case MessageType.REQUEST: // this is a GET request.
// serveGetRequest(cache, dataInputStream, dataOutputStream);
// break;
// }
// } catch (IOException e) {
// e.printStackTrace();
// // todo error handling.
// }
// }
//
// private static void serveGetRequest(Cache cache, DataInputStream dataInputStream,
// DataOutputStream dataOutputStream) throws IOException {
// // GetRequest: Header RegionName Key CallbackArg
// final String regionName = readString(dataInputStream);
// final String key = readString(dataInputStream);
// // todo no callback arg for now
// final Region<Object, Object> region = cache.getRegion(regionName);
// // todo anything more complex?
//
// Object val = region.get(key);
// if (val == null) {
// byte[] bytes = "Entry not found in region.".getBytes();
// dataOutputStream.writeInt(bytes.length); // len
// dataOutputStream.writeByte(19); // ENTRY_NOT_FOUND_EXCEPTION
// dataOutputStream.write(bytes);
// } else {
// byte[] bytes = val.toString().getBytes();
// dataOutputStream.writeInt(bytes.length); // len
// dataOutputStream.writeByte(1); // RESPONSE
// dataOutputStream.write(bytes);
// }
// dataOutputStream.flush();
// }
//
// // response: size responseType requestId
//
// private static void servePutRequest(RequestHeader header, Cache cache,
// DataInputStream dataInputStream, DataOutputStream dataOutputStream) throws IOException {
// String regionName = readString(dataInputStream);
// // assume every object is a string.
//
// String key = readString(dataInputStream);
// // TODO: value header, callback arg?
// String value = readString(dataInputStream);
//
// Region<Object, Object> region = cache.getRegion(regionName);
// region.put(key, value);
//
// dataOutputStream.writeInt(0); // len
// dataOutputStream.writeByte(1); // RESPONSE
// dataOutputStream.writeInt(header.requestId);
// dataOutputStream.flush();
// }
//
// private static String readString(DataInputStream inputStream) throws IOException {
// String s = inputStream.readUTF();
// return s;
// }
// }
