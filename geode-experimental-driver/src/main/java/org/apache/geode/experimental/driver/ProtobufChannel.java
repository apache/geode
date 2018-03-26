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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.util.Objects;
import java.util.Set;

import org.apache.geode.internal.protocol.protobuf.ProtocolVersion;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.ErrorResponse;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message.MessageTypeCase;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorAPI;

class ProtobufChannel {
  /**
   * Socket to a GemFire server that has Protobuf enabled.
   */
  final Socket socket;

  public ProtobufChannel(final Set<InetSocketAddress> locators, String username, String password,
      String keyStorePath, String trustStorePath, String protocols, String ciphers)
      throws GeneralSecurityException, IOException {
    socket = connectToAServer(locators, username, password, keyStorePath, trustStorePath, protocols,
        ciphers);
  }

  public void close() throws IOException {
    socket.close();
  }

  public boolean isClosed() {
    return socket.isClosed();
  }

  private Socket connectToAServer(final Set<InetSocketAddress> locators, String username,
      String password, String keyStorePath, String trustStorePath, String protocols, String ciphers)
      throws GeneralSecurityException, IOException {
    InetSocketAddress server =
        findAServer(locators, username, password, keyStorePath, trustStorePath, protocols, ciphers);
    Socket socket = createSocket(server.getAddress(), server.getPort(), keyStorePath,
        trustStorePath, protocols, ciphers);
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

    if (!Objects.isNull(username)) {
      authenticate(username, password, outputStream, inputStream);
    }

    return socket;
  }

  /**
   * Queries locators for a Geode server that has Protobuf enabled.
   *
   * @return The server chosen by the Locator service for this client
   */
  private InetSocketAddress findAServer(final Set<InetSocketAddress> locators, String username,
      String password, String keyStorePath, String trustStorePath, String protocols, String ciphers)
      throws GeneralSecurityException, IOException {
    IOException lastException = null;

    for (InetSocketAddress locator : locators) {
      Socket locatorSocket = null;
      try {
        locatorSocket = createSocket(locator.getAddress(), locator.getPort(), keyStorePath,
            trustStorePath, protocols, ciphers);

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

        if (!Objects.isNull(username)) {
          authenticate(username, password, outputStream, inputStream);
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
      } finally {
        if (locatorSocket != null) {
          locatorSocket.setSoLinger(true, 0);
          locatorSocket.close();
        }
      }
    }

    if (lastException != null) {
      throw lastException;
    } else {
      throw new IllegalStateException("No locators");
    }
  }

  private void authenticate(String username, String password, OutputStream outputStream,
      InputStream inputStream) throws IOException {
    final ConnectionAPI.AuthenticationRequest.Builder builder =
        ConnectionAPI.AuthenticationRequest.newBuilder();
    builder.putCredentials("security-username", username);
    builder.putCredentials("security-password", password);
    final Message authenticationRequest =
        Message.newBuilder().setAuthenticationRequest(builder).build();
    authenticationRequest.writeDelimitedTo(outputStream);

    final Message authenticationResponseMessage = Message.parseDelimitedFrom(inputStream);
    final ErrorResponse errorResponse = authenticationResponseMessage.getErrorResponse();
    if (!Objects.isNull(errorResponse) && errorResponse.hasError()) {
      throw new IOException("Failed authentication for " + username + ": error code="
          + errorResponse.getError().getErrorCode() + "; error message="
          + errorResponse.getError().getMessage());
    }
    final ConnectionAPI.AuthenticationResponse authenticationResponse =
        authenticationResponseMessage.getAuthenticationResponse();
    if (!Objects.isNull(authenticationResponse) && !authenticationResponse.getAuthenticated()) {
      throw new IOException("Failed authentication for " + username);
    }
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

  private Message readResponse() throws IOException {
    final InputStream inputStream = socket.getInputStream();
    Message response = ClientProtocol.Message.parseDelimitedFrom(inputStream);
    if (response == null) {
      throw new IOException("Unable to parse a response message due to EOF");
    }
    final ErrorResponse errorResponse = response.getErrorResponse();
    if (errorResponse != null && errorResponse.hasError()) {
      throw new IOException(errorResponse.getError().getMessage());
    }
    return response;
  }

  private Socket createSocket(InetAddress host, int port, String keyStorePath,
      String trustStorePath, String protocols, String ciphers)
      throws GeneralSecurityException, IOException {
    return new SocketFactory().setHost(host).setPort(port).setTimeout(5000)
        .setKeyStorePath(keyStorePath).setTrustStorePath(trustStorePath).setProtocols(protocols)
        .setCiphers(ciphers).connect();
  }
}
