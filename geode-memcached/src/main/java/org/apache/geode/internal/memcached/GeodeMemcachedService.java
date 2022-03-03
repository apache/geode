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
package org.apache.geode.internal.memcached;

import org.apache.logging.log4j.Logger;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;
import org.apache.geode.memcached.GemFireMemcachedServer;

/**
 * Service loaded at cache initialization that starts the {@link GemFireMemcachedServer}
 * if {@link ConfigurationProperties#MEMCACHED_PORT} is set.
 */
public class GeodeMemcachedService implements CacheService {
  private static final Logger logger = LogService.getLogger();
  private GemFireMemcachedServer memcachedServer;

  @Override
  public boolean init(Cache cache) {
    InternalCache internalCache = (InternalCache) cache;
    startMemcachedServer(internalCache);

    return true;
  }

  private void startMemcachedServer(InternalCache internalCache) {
    InternalDistributedSystem system = internalCache.getInternalDistributedSystem();
    int port = system.getConfig().getMemcachedPort();
    if (port != 0) {
      String protocol = system.getConfig().getMemcachedProtocol();
      assert protocol != null;
      String bindAddress = system.getConfig().getMemcachedBindAddress();
      assert bindAddress != null;
      if (bindAddress.equals(DistributionConfig.DEFAULT_MEMCACHED_BIND_ADDRESS)) {
        logger.info("Starting GemFireMemcachedServer on port {} for {} protocol",
            new Object[] {port, protocol});
      } else {
        logger.info("Starting GemFireMemcachedServer on bind address {} on port {} for {} protocol",
            new Object[] {bindAddress, port, protocol});
      }
      memcachedServer =
          new GemFireMemcachedServer(bindAddress, port,
              GemFireMemcachedServer.Protocol.valueOf(protocol.toUpperCase()));
      memcachedServer.start();
    }
  }


  @Override
  public void close() {
    stopMemcachedServer();
  }

  private void stopMemcachedServer() {
    if (memcachedServer != null) {
      logger.info("GemFireMemcachedServer is shutting down");
      memcachedServer.shutdown();
    }
  }



  @Override
  public Class<? extends CacheService> getInterface() {
    return GeodeMemcachedService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }
}
