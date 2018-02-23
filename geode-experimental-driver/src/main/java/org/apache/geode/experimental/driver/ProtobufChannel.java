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
package org.apache.geode.experimental.driver;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;

import org.apache.geode.internal.protocol.protobuf.ProtocolVersion;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.ErrorResponse;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message.MessageTypeCase;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorAPI;

class ProtobufChannel {

  private final Set<InetSocketAddress> locators;
  /**
   * Socket to a GemFire server that has Protobuf enabled.
   */
  final Socket socket;

  public ProtobufChannel(final Set<InetSocketAddress> locators) throws IOException {
    this.locators = locators;
    this.socket = connectToAServer();
  }

  Message sendRequest(final Message request, MessageTypeCase expectedResult) throws IOException {
    final OutputStream outputStream = socket.getOutputStream();
    request.writeDelimitedTo(outputStream);
    Message response = readResponse();

    if (!response.getMessageTypeCase().equals(expectedResult)) {
      throw new RuntimeException(
          "Got invalid response for request " + request + ", response " + response);
    }
    return response;
  }

  public void close() throws IOException {
    this.socket.close();
  }

  public boolean isClosed() {
    return this.socket.isClosed();
  }

  private Socket connectToAServer() throws IOException {
    InetSocketAddress server = findAServer();
    Socket socket = new Socket(server.getAddress(), server.getPort());
    socket.setTcpNoDelay(true);
    socket.setSendBufferSize(65535);
    socket.setReceiveBufferSize(65535);

    final OutputStream outputStream = socket.getOutputStream();
    ProtocolVersion.NewConnectionClientVersion.newBuilder()
        .setMajorVersion(ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
        .setMinorVersion(ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE).build()
        .writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    if (!ProtocolVersion.VersionAcknowledgement.parseDelimitedFrom(inputStream)
        .getVersionAccepted()) {
      throw new IOException("Failed protocol version verification.");
    }
    return socket;
  }

  /**
   * Queries locators for a Geode server that has Protobuf enabled.
   * 
   * @return The server chosen by the Locator service for this client
   */
  private InetSocketAddress findAServer() throws IOException {
    IOException lastException = null;

    for (InetSocketAddress locator : locators) {
      try {
        final Socket locatorSocket = new Socket(locator.getAddress(), locator.getPort());

        final OutputStream outputStream = locatorSocket.getOutputStream();
        final InputStream inputStream = locatorSocket.getInputStream();
        ProtocolVersion.NewConnectionClientVersion.newBuilder()
            .setMajorVersion(ProtocolVersion.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
            .setMinorVersion(ProtocolVersion.MinorVersions.CURRENT_MINOR_VERSION_VALUE).build()
            .writeDelimitedTo(outputStream);

        // The locator does not currently send a reply to the ProtocolVersion...
        if (!ProtocolVersion.VersionAcknowledgement.parseDelimitedFrom(inputStream)
            .getVersionAccepted()) {
          throw new IOException("Failed ProtocolVersion.");
        }

        ClientProtocol.Message.newBuilder()
            .setGetServerRequest(LocatorAPI.GetServerRequest.newBuilder()).build()
            .writeDelimitedTo(outputStream);

        ClientProtocol.Message response = ClientProtocol.Message.parseDelimitedFrom(inputStream);
        ClientProtocol.ErrorResponse errorResponse = response.getErrorResponse();

        if (errorResponse != null && errorResponse.hasError()) {
          throw new IOException(
              "Error finding server: error code= " + errorResponse.getError().getErrorCode()
                  + "; error message=" + errorResponse.getError().getMessage());
        }

        LocatorAPI.GetServerResponse getServerResponse = response.getGetServerResponse();

        BasicTypes.Server server = getServerResponse.getServer();
        return new InetSocketAddress(server.getHostname(), server.getPort());
      } catch (IOException e) {
        lastException = e;
      }
    }

    if (lastException != null) {
      throw lastException;
    } else {
      throw new IllegalStateException("No locators");
    }
  }

  private Message readResponse() throws IOException {
    final InputStream inputStream = socket.getInputStream();
    Message response = ClientProtocol.Message.parseDelimitedFrom(inputStream);
    final ErrorResponse errorResponse = response.getErrorResponse();
    if (errorResponse != null && errorResponse.hasError()) {
      throw new IOException(errorResponse.getError().getMessage());
    }
    return response;
  }

}
