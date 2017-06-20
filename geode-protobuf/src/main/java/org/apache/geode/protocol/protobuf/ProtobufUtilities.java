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

import com.google.protobuf.ByteString;

import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;

public abstract class ProtobufUtilities {
  public static BasicTypes.EncodedValue getEncodedValue(BasicTypes.EncodingType resultEncodingType,
      byte[] resultEncodedValue) {
    return BasicTypes.EncodedValue.newBuilder().setEncodingType(resultEncodingType)
        .setValue(ByteString.copyFrom(resultEncodedValue)).build();
  }

  public static ClientProtocol.Message wrapResponseWithDefaultHeader(
      ClientProtocol.Response response) {
    return ClientProtocol.Message.newBuilder()
        .setMessageHeader(ClientProtocol.MessageHeader.newBuilder()).setResponse(response).build();
  }
}
