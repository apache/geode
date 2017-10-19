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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.ClientProtocolMessageHandler;
import org.apache.geode.internal.protocol.MessageExecutionContext;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.internal.protocol.protobuf.registry.ProtobufOperationContextRegistry;
import org.apache.geode.internal.protocol.protobuf.serializer.ProtobufProtocolSerializer;
import org.apache.geode.internal.protocol.statistics.ProtocolClientStatistics;
import org.apache.geode.internal.protocol.protobuf.utilities.ProtobufUtilities;

/**
 * This object handles an incoming stream containing protobuf messages. It parses the protobuf
 * messages, hands the requests to an appropriate handler, wraps the response in a protobuf message,
 * and then pushes it to the output stream.
 */
@Experimental
public class ProtobufStreamProcessor implements ClientProtocolMessageHandler {
  private final ProtobufProtocolSerializer protobufProtocolSerializer;
  private final ProtobufOpsProcessor protobufOpsProcessor;
  private static final Logger logger = LogService.getLogger();

  public ProtobufStreamProcessor() {
    protobufProtocolSerializer = new ProtobufProtocolSerializer();
    protobufOpsProcessor = new ProtobufOpsProcessor(new ProtobufSerializationService(),
        new ProtobufOperationContextRegistry());
  }

  @Override
  public void receiveMessage(InputStream inputStream, OutputStream outputStream,
      MessageExecutionContext executionContext) throws IOException {
    try {
      processOneMessage(inputStream, outputStream, executionContext);
    } catch (InvalidProtocolMessageException e) {
      throw new IOException(e);
    }
  }

  private void processOneMessage(InputStream inputStream, OutputStream outputStream,
      MessageExecutionContext executionContext)
      throws InvalidProtocolMessageException, IOException {
    ClientProtocol.Message message = protobufProtocolSerializer.deserialize(inputStream);
    if (message == null) {
      String errorMessage = "Tried to deserialize protobuf message at EOF";
      logger.debug(errorMessage);
      throw new EOFException(errorMessage);
    }
    ProtocolClientStatistics statistics = executionContext.getStatistics();
    statistics.messageReceived(message.getSerializedSize());

    ClientProtocol.Request request = message.getRequest();
    ClientProtocol.Response response = protobufOpsProcessor.process(request, executionContext);
    ClientProtocol.Message responseMessage = ProtobufUtilities.createProtobufResponse(response);
    statistics.messageSent(responseMessage.getSerializedSize());
    protobufProtocolSerializer.serialize(responseMessage, outputStream);
  }
}
