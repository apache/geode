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

package org.apache.geode.management.internal.api;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.internal.exceptions.InvalidContextException;

public class ClusterManagementServiceProvider {

  public static ClusterManagementService getService(ClusterManagementContext ctx) {
    switch (ctx) {
      case FOR_LOCATOR:
        return InternalLocator.getLocator().getClusterManagementService();
      case FOR_SERVER:
        return new ServerClusterManagementService();
      case FOR_CLIENT:
        return new GeodeClientClusterManagementService();
      default:
        throw new IllegalArgumentException(
            "Context must be one of FOR_LOCATOR, FOR_SERVER or FOR_CLIENT");
    }
  }

  public static ClusterManagementService getService() {
    if (InternalLocator.getLocator() != null) {
      return getService(ClusterManagementContext.FOR_LOCATOR);
    }

    try {
      Cache cache = CacheFactory.getAnyInstance();
      if (cache != null && cache.isServer()) {
        return getService(ClusterManagementContext.FOR_SERVER);
      }

      ClientCache clientCache = ClientCacheFactory.getAnyInstance();
      if (clientCache != null) {
        return getService(ClusterManagementContext.FOR_CLIENT);
      }
    } catch (CacheClosedException e) {
      throw new InvalidContextException("ClusterManagementService.create() " +
          "must be executed on one of locator, server or client cache VMs");
    }

    return null;
  }
}
