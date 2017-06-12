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
package org.apache.geode.protocol.handler;

import org.apache.geode.ProtobufUtilities;
import org.apache.geode.cache.Cache;
import org.apache.geode.protocol.OpsProcessor;
import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.handler.protobuf.ProtobufProtocolHandler;
import org.apache.geode.protocol.operations.registry.OperationsHandlerRegistry;
import org.apache.geode.protocol.operations.registry.exception.OperationHandlerAlreadyRegisteredException;
import org.apache.geode.protocol.operations.registry.exception.OperationHandlerNotRegisteredException;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.serialization.ProtobufSerializationService;
import org.apache.geode.serialization.exception.UnsupportedEncodingTypeException;
import org.apache.geode.serialization.registry.exception.CodecAlreadyRegisteredForTypeException;
import org.apache.geode.serialization.registry.exception.CodecNotRegisteredForTypeException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ProtobufStreamProcessor {
  ProtobufProtocolHandler protobufProtocolHandler;
  OperationsHandlerRegistry registry;
  ProtobufSerializationService protobufSerializationService;
  OpsProcessor opsProcessor;

  public ProtobufStreamProcessor()
      throws OperationHandlerAlreadyRegisteredException, CodecAlreadyRegisteredForTypeException {
    protobufProtocolHandler = new ProtobufProtocolHandler();
    registry = new OperationsHandlerRegistry();
    protobufSerializationService = new ProtobufSerializationService();
    opsProcessor = new OpsProcessor(registry, protobufSerializationService);
  }

  public void processOneMessage(InputStream inputStream, OutputStream outputStream, Cache cache)
      throws InvalidProtocolMessageException, OperationHandlerNotRegisteredException,
      UnsupportedEncodingTypeException, CodecNotRegisteredForTypeException, IOException {
    ClientProtocol.Message message = protobufProtocolHandler.deserialize(inputStream);

    ClientProtocol.Request request = message.getRequest();
    ClientProtocol.Response response = opsProcessor.process(request, cache);

    ClientProtocol.Message responseMessage =
        ProtobufUtilities.wrapResponseWithDefaultHeader(response);
    protobufProtocolHandler.serialize(responseMessage, outputStream);
  }
}
