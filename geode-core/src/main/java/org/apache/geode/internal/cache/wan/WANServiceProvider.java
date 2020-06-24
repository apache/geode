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
package org.apache.geode.internal.cache.wan;

import java.util.Set;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.cache.client.internal.locator.wan.LocatorMembershipListener;
import org.apache.geode.cache.wan.GatewayReceiverFactory;
import org.apache.geode.cache.wan.GatewaySenderFactory;
import org.apache.geode.distributed.internal.WanLocatorDiscoverer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.wan.spi.WANFactory;
import org.apache.geode.services.module.ModuleService;
import org.apache.geode.services.result.ModuleServiceResult;

public class WANServiceProvider {
  @Immutable
  private static WANFactory factory;

  private static void setup(ModuleService moduleService) {
    if (factory == null) {
      ModuleServiceResult<Set<WANFactory>> loadServiceResult =
          moduleService.loadService(WANFactory.class);
      if (loadServiceResult.isSuccessful()) {
        for (WANFactory wanFactory : loadServiceResult.getMessage()) {
          factory = wanFactory;
          factory.initialize();
          break;
        }
      } else {
        factory = null;
      }
    }
  }

  public static GatewaySenderFactory createGatewaySenderFactory(InternalCache cache,
      ModuleService moduleService) {
    setup(moduleService);
    if (factory == null) {
      throw new IllegalStateException("WAN service is not available.");
    }
    return factory.createGatewaySenderFactory(cache);

  }

  public static GatewayReceiverFactory createGatewayReceiverFactory(InternalCache cache,
      ModuleService moduleService) {
    setup(moduleService);
    if (factory == null) {
      throw new IllegalStateException("WAN service is not available.");
    }
    return factory.createGatewayReceiverFactory(cache);
  }

  public static WanLocatorDiscoverer createLocatorDiscoverer(ModuleService moduleService) {
    setup(moduleService);
    if (factory == null) {
      return null;
    }
    return factory.createLocatorDiscoverer();
  }

  public static LocatorMembershipListener createLocatorMembershipListener(
      ModuleService moduleService) {
    setup(moduleService);
    if (factory == null) {
      return null;
    }
    return factory.createLocatorMembershipListener();
  }

  private WANServiceProvider() {}
}
