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
package org.apache.geode.internal.cache.client.protobuf;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorAPI;

@Experimental
public class CacheFactory {
  public static class ServerInfo {
    public String host;
    public int port;

    public ServerInfo(String host, int port) {
      this.host = host;
      this.port = port;
    }
  }

  String host;
  int port;

  public CacheFactory() {
    // NOP
  }

  public CacheFactory addLocator(String host, int port) {
    this.host = host;
    this.port = port;
    return this;
  }

  public Collection<ServerInfo> getAvailableServers() throws Exception {
    final Socket locatorSocket = new Socket(host, port);

    OutputStream outputStream = locatorSocket.getOutputStream();
    // Once GEODE-4010 is fixed, this can revert to just the communication mode and major version.
    outputStream.write(0x00); // NON_GOSSIP_REQUEST_VERSION
    outputStream.write(0x00); // NON_GOSSIP_REQUEST_VERSION
    outputStream.write(0x00); // NON_GOSSIP_REQUEST_VERSION
    outputStream.write(0x00); // NON_GOSSIP_REQUEST_VERSION
    outputStream.write(0x6E); // Magic byte
    outputStream.write(0x01); // Major version

    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setGetAvailableServersRequest(LocatorAPI.GetAvailableServersRequest.newBuilder()))
        .build().writeDelimitedTo(outputStream);

    InputStream inputStream = locatorSocket.getInputStream();
    LocatorAPI.GetAvailableServersResponse getAvailableServersResponse = ClientProtocol.Message
        .parseDelimitedFrom(inputStream).getResponse().getGetAvailableServersResponse();
    if (getAvailableServersResponse.getServersCount() < 1) {
      throw new Exception("No available servers");
    }

    ArrayList<ServerInfo> availableServers =
        new ArrayList<>(getAvailableServersResponse.getServersCount());
    for (int i = 0; i < getAvailableServersResponse.getServersCount(); ++i) {
      final BasicTypes.Server server = getAvailableServersResponse.getServers(i);
      availableServers.add(new ServerInfo(server.getHostname(), server.getPort()));
    }
    return availableServers;
  }

  public Cache connect() throws Exception {
    final ServerInfo arbitraryServer = getAvailableServers().iterator().next();
    return new Cache(arbitraryServer.host, arbitraryServer.port);
  }
}
