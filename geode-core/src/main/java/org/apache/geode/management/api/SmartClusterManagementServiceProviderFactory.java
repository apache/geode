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

package org.apache.geode.management.api;

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.NotImplementedException;
import org.springframework.http.client.ClientHttpRequestFactory;

import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.client.ClusterManagementServiceProvider;
import org.apache.geode.management.spi.ClusterManagementServiceProviderFactory;

public class SmartClusterManagementServiceProviderFactory implements
    ClusterManagementServiceProviderFactory {

  @Override
  public List<String> supportedContexts() {
    return Arrays.asList(ClusterManagementServiceProvider.GEODE_CLIENT_CONTEXT,
        ClusterManagementServiceProvider.SERVER_CONTEXT);
  }

  @Override
  public ClusterManagementService create() {
    if (InternalLocator.getLocator() != null) {
      return InternalLocator.getLocator().getClusterManagementService();
    }

    throw new IllegalStateException("This method is only implemented on locators for now.");

    // try {
    // Cache cache = CacheFactory.getAnyInstance();
    // if (cache != null && cache.isServer()) {
    // return getService(ClusterManagementContext.FOR_SERVER);
    // }
    //
    // ClientCache clientCache = ClientCacheFactory.getAnyInstance();
    // if (clientCache != null) {
    // return getService(ClusterManagementContext.FOR_CLIENT);
    // }
    // } catch (CacheClosedException e) {
    // throw new InvalidContextException("ClusterManagementService.create() " +
    // "must be executed on one of locator, server or client cache VMs");
    // }

  }

  @Override
  public ClusterManagementService create(String clusterUrl) {
    throw new NotImplementedException("Pardon our dust. Under construction");
  }

  @Override
  public ClusterManagementService create(ClientHttpRequestFactory requestFactory) {
    throw new NotImplementedException("Pardon our dust. Under construction");
  }

}
