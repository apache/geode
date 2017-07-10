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
package org.apache.geode.protocol.protobuf.serializer;

import org.apache.geode.protocol.exception.InvalidProtocolMessageException;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.serializer.ProtocolSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ProtobufProtocolSerializer implements ProtocolSerializer<ClientProtocol.Message> {
  @Override
  public ClientProtocol.Message deserialize(InputStream inputStream)
      throws InvalidProtocolMessageException {
    try {
      return ClientProtocol.Message.parseDelimitedFrom(inputStream);
    } catch (IOException e) {
      throw new InvalidProtocolMessageException("Failed to parse Protobuf Message", e);
    }
  }

  @Override
  public void serialize(ClientProtocol.Message inputMessage, OutputStream outputStream)
      throws IOException {
    inputMessage.writeDelimitedTo(outputStream);
  }
}
