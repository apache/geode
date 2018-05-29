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
package org.apache.geode.internal.protocol.protobuf.security;

import static org.apache.geode.security.ResourcePermission.ALL;
import static org.apache.geode.security.ResourcePermission.Operation.READ;
import static org.apache.geode.security.ResourcePermission.Resource.CLUSTER;

import java.util.Set;

import org.apache.geode.cache.client.internal.locator.ClientConnectionRequest;
import org.apache.geode.cache.client.internal.locator.ClientConnectionResponse;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.ServerLocation;

public class SecureLocatorImpl implements SecureLocator {
  private final Locator locator;
  private final Security security;

  public SecureLocatorImpl(Locator locator, Security security) {
    this.locator = locator;
    this.security = security;
  }

  @Override
  public ServerLocation findServer(Set<ServerLocation> excludedServers, String serverGroup) {
    security.authorize(CLUSTER, READ, ALL, ALL);
    InternalLocator internalLocator = (InternalLocator) locator;

    // In order to ensure that proper checks are performed on the request we will use
    // the locator's processRequest() API. We assume that all servers have Protobuf
    // enabled.
    ClientConnectionRequest clientConnectionRequest =
        new ClientConnectionRequest(excludedServers, serverGroup);
    ClientConnectionResponse connectionResponse = (ClientConnectionResponse) internalLocator
        .getServerLocatorAdvisee().processRequest(clientConnectionRequest);

    ServerLocation serverLocation = null;
    if (connectionResponse != null && connectionResponse.hasResult()) {
      serverLocation = connectionResponse.getServer();
    }

    return serverLocation;

  }
}
