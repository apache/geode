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
package org.apache.geode.redis.internal;


import org.apache.logging.log4j.Logger;

import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ResourceEvent;
import org.apache.geode.distributed.internal.ResourceEventsListener;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.NullRedisData;
import org.apache.geode.redis.internal.data.RedisHash;
import org.apache.geode.redis.internal.data.RedisKey;
import org.apache.geode.redis.internal.data.RedisSet;
import org.apache.geode.redis.internal.data.RedisString;
import org.apache.geode.redis.internal.executor.string.SetOptions;

public class GeodeRedisService implements CacheService, ResourceEventsListener {
  private static final Logger logger = LogService.getLogger();
  private GeodeRedisServer redisServer;
  private InternalCache cache;

  @Override
  public boolean init(Cache cache) {
    this.cache = (InternalCache) cache;
    if (!this.cache.getInternalDistributedSystem().getConfig().getRedisEnabled()) {
      return false;
    }

    this.cache.getInternalDistributedSystem().addResourceListener(this);
    registerDataSerializables();

    return true;
  }

  private void registerDataSerializables() {
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_KEY,
        RedisKey.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_BYTE_ARRAY_WRAPPER,
        ByteArrayWrapper.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_SET_ID,
        RedisSet.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_STRING_ID,
        RedisString.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_HASH_ID,
        RedisHash.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_NULL_DATA_ID,
        NullRedisData.class);
    InternalDataSerializer.getDSFIDSerializer().registerDSFID(
        DataSerializableFixedID.REDIS_SET_OPTIONS_ID,
        SetOptions.class);
  }

  @Override
  public void close() {
    stopRedisServer();
  }

  @Override
  public void handleEvent(ResourceEvent event, Object resource) {
    if (event.equals(ResourceEvent.CLUSTER_CONFIGURATION_APPLIED) && resource == cache) {
      startRedisServer(cache);
    }
  }

  private void startRedisServer(InternalCache cache) {
    InternalDistributedSystem system = cache.getInternalDistributedSystem();

    if (system.getConfig().getRedisEnabled()) {
      int port = system.getConfig().getRedisPort();
      String bindAddress = system.getConfig().getRedisBindAddress();
      assert bindAddress != null;

      logger.info(
          String.format("Starting GeodeRedisServer on bind address %s on port %s",
              new Object[] {bindAddress, port}));

      this.redisServer = new GeodeRedisServer(bindAddress, port, cache);
    }
  }

  private void stopRedisServer() {
    if (this.redisServer != null) {
      this.redisServer.shutdown();
    }
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return GeodeRedisService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }

  public int getPort() {
    return redisServer.getPort();
  }

  @VisibleForTesting
  public Long getDataStoreBytesInUseForDataRegion() {
    return redisServer.getDataStoreBytesInUseForDataRegion();
  }

  public void setEnableUnsupported(boolean unsupported) {
    redisServer.setAllowUnsupportedCommandsSystemProperty(unsupported);
  }
}
