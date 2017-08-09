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
package org.apache.geode.protocol.protobuf;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.tier.sockets.ClientProtocolMessageHandler;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.protobuf.registry.OperationContextRegistry;
import org.apache.geode.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.protocol.protobuf.utilities.ProtobufUtilities;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;

/**
 * This object handles an incoming stream containing protobuf messages. It parses the protobuf
 * messages, hands the requests to an appropriate handler, wraps the response in a protobuf message,
 * and then pushes it to the output stream.
 */
@Experimental
public class ProtobufStreamProcessor implements ClientProtocolMessageHandler {
  private final ProtobufProtocolSerializer protobufProtocolSerializer;
  private final ProtobufOpsProcessor protobufOpsProcessor;

  public ProtobufStreamProcessor() throws CodecAlreadyRegisteredForTypeException {
    protobufProtocolSerializer = new ProtobufProtocolSerializer();
    protobufOpsProcessor = new ProtobufOpsProcessor(new ProtobufSerializationService(),
        new OperationContextRegistry());
  }

  public void processOneMessage(InputStream inputStream, OutputStream outputStream, Cache cache)
      throws InvalidProtocolMessageException, IOException {
    ClientProtocol.Message message = protobufProtocolSerializer.deserialize(inputStream);
    if (message == null) {
      throw new EOFException("Tried to deserialize protobuf message at EOF");
    }

    ClientProtocol.Request request = message.getRequest();
    ClientProtocol.Response response = protobufOpsProcessor.process(request, cache);
    ClientProtocol.MessageHeader responseHeader =
        ProtobufUtilities.createMessageHeaderForRequest(message);
    ClientProtocol.Message responseMessage =
        ProtobufUtilities.createProtobufResponse(responseHeader, response);
    protobufProtocolSerializer.serialize(responseMessage, outputStream);
  }

  @Override
  public void receiveMessage(InputStream inputStream, OutputStream outputStream,
      InternalCache cache) throws IOException {
    try {
      processOneMessage(inputStream, outputStream, cache);
    } catch (InvalidProtocolMessageException e) {
      throw new IOException(e);
    }
  }
}
