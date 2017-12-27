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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.internal.protocol.protobuf.ProtocolVersion;
import org.apache.geode.internal.protocol.protobuf.v1.BasicTypes;
import org.apache.geode.internal.protocol.protobuf.v1.ClientProtocol;
import org.apache.geode.internal.protocol.protobuf.v1.LocatorAPI;
import org.apache.geode.internal.protocol.protobuf.v1.RegionAPI;

/**
 * Implements the behaviors of a driver for communicating with a GemFire server by way of the new
 * protocol.
 *
 * <strong>This code is an experimental prototype and is presented "as is" with no warranty,
 * suitability, or fitness of purpose implied.</strong>
 */
@Experimental
public class ProtobufDriver implements Driver {
  /**
   * Set of Internet-address-or-host-name/port pairs of the locators to use to find GemFire servers
   * that have Protobuf enabled.
   */
  private final Set<InetSocketAddress> locators;

  /**
   * Socket to a GemFire locator that has Protobuf enabled.
   */
  private final Socket socket;

  /**
   * Creates a driver implementation that communicates via <code>socket</code> to a GemFire locator.
   *
   * @param locators Set of Internet-address-or-host-name/port pairs of the locators to use to find
   *        GemFire servers that have Protobuf enabled.
   * @throws IOException
   */
  ProtobufDriver(Set<InetSocketAddress> locators) throws IOException {
    this.locators = locators;
    Collection<InetSocketAddress> servers = getAvailableServers();
    InetSocketAddress anyServer = servers.iterator().next();
    socket = new Socket(anyServer.getAddress(), anyServer.getPort());

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
  }

  /**
   * Retrieves a set of unique names of regions in the GemFire server to which this driver is
   * connected.
   *
   * @return Set of strings of names that uniquely identify regions.
   * @throws IOException
   */
  @Override
  public Set<String> getRegionNames() throws IOException {
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

  /**
   * Creates an implementation of the region interface for the region with the unique name of
   * <code>regionName</code>.
   *
   * @param regionName String that uniquely identifies the region.
   * @param <K> Type of region keys.
   * @param <V> Type of region values.
   * @return
   */
  @Override
  public <K, V> Region<K, V> getRegion(String regionName) {
    return new ProtobufRegion(regionName, socket);
  }

  /**
   * Queries a locator for the GemFire servers that have Protobuf enabled.
   *
   * @return Set of Internet-address-or-host-name/port pairs of the GemFire servers that have
   *         Protobuf enabled.
   * @throws IOException
   */
  private Collection<InetSocketAddress> getAvailableServers() throws IOException {
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
            .setRequest(ClientProtocol.Request.newBuilder()
                .setGetAvailableServersRequest(LocatorAPI.GetAvailableServersRequest.newBuilder()))
            .build().writeDelimitedTo(outputStream);

        LocatorAPI.GetAvailableServersResponse getAvailableServersResponse = ClientProtocol.Message
            .parseDelimitedFrom(inputStream).getResponse().getGetAvailableServersResponse();
        if (getAvailableServersResponse.getServersCount() < 1) {
          continue;
        }

        ArrayList<InetSocketAddress> availableServers =
            new ArrayList<>(getAvailableServersResponse.getServersCount());
        for (int i = 0; i < getAvailableServersResponse.getServersCount(); ++i) {
          final BasicTypes.Server server = getAvailableServersResponse.getServers(i);
          availableServers.add(new InetSocketAddress(server.getHostname(), server.getPort()));
        }
        return availableServers;
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
}
