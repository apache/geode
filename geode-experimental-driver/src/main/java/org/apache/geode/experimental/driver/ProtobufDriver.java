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
import java.net.InetSocketAddress;
import java.security.GeneralSecurityException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol.Message.MessageTypeCase;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI.GetRegionNamesRequest;

/**
 * Implements the behaviors of a driver for communicating with a GemFire server by way of the new
 * protocol.
 *
 * <strong>This code is an experimental prototype and is presented "as is" with no warranty,
 * suitability, or fitness of purpose implied.</strong>
 */
@Experimental
public class ProtobufDriver implements Driver {

  private final ProtobufChannel channel;
  private ValueEncoder valueEncoder;

  /**
   * Creates a driver implementation that communicates via <code>socket</code> to a GemFire locator.
   *
   * @param locators Set of Internet-address-or-host-name/port pairs of the locators to use to find
   *        GemFire servers that have Protobuf enabled.
   * @param username User identity as a string; may be <code>null</code>.
   * @param password User proof as a string; may be <code>null</code>.
   * @param keyStorePath Path to SSL key store; SSL is <em>not</em> used if <code>null</code>.
   * @param trustStorePath Path to SSL trust store; SSL is <em>not</em> used if <code>null</code>.
   * @param protocols Space-separated list of the SSL protocols to enable.
   * @param ciphers Space-separated list of the SSL cipher suites to enable.
   */
  ProtobufDriver(Set<InetSocketAddress> locators, String username, String password,
      String keyStorePath, String trustStorePath, String protocols, String ciphers,
      ValueSerializer serializer) throws GeneralSecurityException, IOException {
    channel = new ProtobufChannel(locators, username, password, keyStorePath, trustStorePath,
        protocols, ciphers, serializer);
    valueEncoder = new ValueEncoder(serializer);
  }

  @Override
  public Set<String> getRegionNames() throws IOException {
    Set<String> regionNames = new HashSet<>();

    final Message request =
        Message.newBuilder().setGetRegionNamesRequest(GetRegionNamesRequest.newBuilder()).build();

    final RegionAPI.GetRegionNamesResponse getRegionNamesResponse = channel
        .sendRequest(request, MessageTypeCase.GETREGIONNAMESRESPONSE).getGetRegionNamesResponse();
    for (int i = 0; i < getRegionNamesResponse.getRegionsCount(); ++i) {
      regionNames.add(getRegionNamesResponse.getRegions(i));
    }

    return regionNames;
  }

  @Override
  public <K, V> Region<K, V> getRegion(String regionName) {
    return new ProtobufRegion<>(regionName, channel, valueEncoder);
  }

  @Override
  public QueryService getQueryService() {
    return new ProtobufQueryService(channel, valueEncoder);
  }

  @Override
  public FunctionService getFunctionService() {
    return new ProtobufFunctionService(channel, valueEncoder);
  }

  @Override
  public void close() {
    try {
      final Message disconnectClientRequest = ClientProtocol.Message.newBuilder()
          .setDisconnectClientRequest(
              ConnectionAPI.DisconnectClientRequest.newBuilder().setReason("Driver closed"))
          .build();
      final ConnectionAPI.DisconnectClientResponse disconnectClientResponse =
          channel.sendRequest(disconnectClientRequest, MessageTypeCase.DISCONNECTCLIENTRESPONSE)
              .getDisconnectClientResponse();
      if (Objects.isNull(disconnectClientResponse)) {
        // The server did not acknowledge the disconnect request; ignore for now.
      }
    } catch (IOException ioe) {
      // NOP
    } finally {
      try {
        channel.close();
      } catch (IOException e) {
        // ignore
      }
    }
  }

  @Override
  public boolean isConnected() {
    return !channel.isClosed();
  }

}
