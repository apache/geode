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
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.ConnectionAPI;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;

@Experimental
public class Cache {
  final Socket socket;

  Cache(String host, int port) throws Exception {
    socket = new Socket(host, port);

    final OutputStream outputStream = socket.getOutputStream();
    outputStream.write(0x6E); // Magic byte
    outputStream.write(0x01); // Major version

    final InputStream inputStream = socket.getInputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setHandshakeRequest(ConnectionAPI.HandshakeRequest.newBuilder()
                .setMajorVersion(ConnectionAPI.MajorVersions.CURRENT_MAJOR_VERSION_VALUE)
                .setMinorVersion(ConnectionAPI.MinorVersions.CURRENT_MINOR_VERSION_VALUE)))
        .build().writeDelimitedTo(outputStream);

    if (!ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse().getHandshakeResponse()
        .getHandshakePassed()) {
      throw new Exception("Failed handshake.");
    }
  }

  public Set<String> getRegionNames() throws Exception {
    Set<String> regionNames = new HashSet<>();

    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setGetRegionNamesRequest(RegionAPI.GetRegionNamesRequest.newBuilder()))
        .build().writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    final RegionAPI.GetRegionNamesResponse getRegionNamesResponse = ClientProtocol.Message
        .parseDelimitedFrom(inputStream).getResponse().getGetRegionNamesResponse();
    for (int i = 0; i < getRegionNamesResponse.getRegionsCount(); ++i) {
      regionNames.add(getRegionNamesResponse.getRegions(i));
    }

    return regionNames;
  }

  public RegionAttributes getRegionAttributes(String regionName) throws Exception {
    final OutputStream outputStream = socket.getOutputStream();
    ClientProtocol.Message.newBuilder()
        .setRequest(ClientProtocol.Request.newBuilder()
            .setGetRegionRequest(RegionAPI.GetRegionRequest.newBuilder().setRegionName(regionName)))
        .build().writeDelimitedTo(outputStream);

    final InputStream inputStream = socket.getInputStream();
    return new RegionAttributes(ClientProtocol.Message.parseDelimitedFrom(inputStream).getResponse()
        .getGetRegionResponse().getRegion());
  }

  public <K, V> Region<K, V> getRegion(String regionName) {
    return new Region(regionName, socket);
  }

  public class RegionAttributes {
    public String name;
    public String dataPolicy;
    public String scope;
    public String keyConstraint;
    public String valueConstraint;
    public boolean persisted;
    public long size;

    public RegionAttributes(BasicTypes.Region region) {
      this.name = region.getName();
      this.dataPolicy = region.getDataPolicy();
      this.scope = region.getScope();
      this.keyConstraint = region.getKeyConstraint();
      this.valueConstraint = region.getValueConstraint();
      this.persisted = region.getPersisted();
      this.size = region.getSize();
    }
  }
}
