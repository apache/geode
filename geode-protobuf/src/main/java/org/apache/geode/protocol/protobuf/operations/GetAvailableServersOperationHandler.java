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
package org.apache.geode.protocol.protobuf.operations;

import org.apache.commons.lang.StringUtils;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.client.internal.locator.GetAllServersRequest;
import org.apache.geode.cache.client.internal.locator.GetAllServersResponse;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ServerLocation;
import org.apache.geode.distributed.internal.tcpserver.TcpClient;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;
import org.apache.geode.protocol.operations.OperationHandler;
import org.apache.geode.protocol.protobuf.BasicTypes;
import org.apache.geode.protocol.protobuf.Failure;
import org.apache.geode.protocol.protobuf.Result;
import org.apache.geode.protocol.protobuf.ServerAPI;
import org.apache.geode.protocol.protobuf.Success;
import org.apache.geode.serialization.SerializationService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.stream.Collectors;

public class GetAvailableServersOperationHandler implements
    OperationHandler<ServerAPI.GetAvailableServersRequest, ServerAPI.GetAvailableServersResponse> {

  @Override
  public Result<ServerAPI.GetAvailableServersResponse> process(
      SerializationService serializationService, ServerAPI.GetAvailableServersRequest request,
      Cache cache) {

    InternalDistributedSystem distributedSystem =
        (InternalDistributedSystem) cache.getDistributedSystem();
    Properties properties = distributedSystem.getProperties();
    String locatorsString = properties.getProperty(ConfigurationProperties.LOCATORS);

    HashSet<DistributionLocatorId> locators = new HashSet();
    StringTokenizer stringTokenizer = new StringTokenizer(locatorsString, ",");
    while (stringTokenizer.hasMoreTokens()) {
      String locator = stringTokenizer.nextToken();
      if (StringUtils.isNotEmpty(locator)) {
        locators.add(new DistributionLocatorId(locator));
      }
    }

    TcpClient tcpClient = getTcpClient();
    for (DistributionLocatorId locator : locators) {
      try {
        return getGetAvailableServersFromLocator(tcpClient, locator.getHost());
      } catch (IOException | ClassNotFoundException e) {
        // try the next locator
      }
    }
    return Failure
        .of(BasicTypes.ErrorResponse.newBuilder().setMessage("Unable to find a locator").build());
  }

  private Result<ServerAPI.GetAvailableServersResponse> getGetAvailableServersFromLocator(
      TcpClient tcpClient, InetSocketAddress address) throws IOException, ClassNotFoundException {
    GetAllServersResponse getAllServersResponse = (GetAllServersResponse) tcpClient
        .requestToServer(address, new GetAllServersRequest(), 1000, true);
    Collection<BasicTypes.Server> servers =
        (Collection<BasicTypes.Server>) getAllServersResponse.getServers().stream()
            .map(serverLocation -> getServerProtobufMessage((ServerLocation) serverLocation))
            .collect(Collectors.toList());
    ServerAPI.GetAvailableServersResponse.Builder builder =
        ServerAPI.GetAvailableServersResponse.newBuilder().addAllServers(servers);
    return Success.of(builder.build());
  }

  protected TcpClient getTcpClient() {
    return new TcpClient();
  }

  private BasicTypes.Server getServerProtobufMessage(ServerLocation serverLocation) {
    BasicTypes.Server.Builder serverBuilder = BasicTypes.Server.newBuilder();
    serverBuilder.setHostname(serverLocation.getHostName()).setPort(serverLocation.getPort());
    return serverBuilder.build();
  }
}
