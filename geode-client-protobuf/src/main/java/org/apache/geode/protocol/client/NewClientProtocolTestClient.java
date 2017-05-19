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

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import org.apache.geode.internal.cache.tier.sockets.AcceptorImpl;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.ClientProtocol;
import org.apache.geode.protocol.protobuf.ClientProtocol.Message;
import org.apache.geode.protocol.protobuf.RegionAPI;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Random;


public class NewClientProtocolTestClient implements AutoCloseable {
  private static SocketChannel socketChannel;
  private final OutputStream outputStream;
  private final InputStream inputStream;

  public NewClientProtocolTestClient(String hostname, int port) throws IOException {
    socketChannel = SocketChannel.open(new InetSocketAddress(hostname, port));
    inputStream = socketChannel.socket().getInputStream();
    outputStream = socketChannel.socket().getOutputStream();

    sendHeader(outputStream);
  }

  @Override
  public void close() throws IOException {
    socketChannel.close();
  }

  private static void sendHeader(OutputStream outputStream) throws IOException {
    outputStream.write(AcceptorImpl.CLIENT_TO_SERVER_NEW_PROTOCOL);
  }

  public Message blockingSendMessage(Message message) throws IOException {
    message.writeDelimitedTo(outputStream);
    outputStream.flush();

    return ClientProtocol.Message.parseDelimitedFrom(inputStream);
  }

  void printResponse(Message response) {
    System.out.println("response = " + response.toString());
  }

  private Message generateMessage() {
    Random random = new Random();
    ClientProtocol.MessageHeader.Builder messageHeader =
        ClientProtocol.MessageHeader.newBuilder().setCorrelationId(random.nextInt());
    // .setSize() //we don't need to set the size because Protobuf will handle the message frame

    BasicTypes.Key.Builder key =
        BasicTypes.Key.newBuilder().setKey(ByteString.copyFrom(createByteArrayOfSize(64)));

    BasicTypes.Value.Builder value =
        BasicTypes.Value.newBuilder().setValue(ByteString.copyFrom(createByteArrayOfSize(512)));

    RegionAPI.PutRequest.Builder putRequestBuilder =
        RegionAPI.PutRequest.newBuilder().setRegionName("TestRegion")
            .setEntry(BasicTypes.Entry.newBuilder().setKey(key).setValue(value));

    ClientProtocol.Request.Builder request =
        ClientProtocol.Request.newBuilder().setPutRequest(putRequestBuilder);

    Message.Builder message =
        Message.newBuilder().setMessageHeader(messageHeader).setRequest(request);

    return message.build();
  }

  private static byte[] createByteArrayOfSize(int msgSize) {
    byte[] array = new byte[msgSize];
    for (int i = 0; i < msgSize; i++) {
      array[i] = 'a';
    }
    return array;
  }

}
