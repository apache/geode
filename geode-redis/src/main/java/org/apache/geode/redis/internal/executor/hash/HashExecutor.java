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
package org.apache.geode.redis.internal.executor.hash;

import static java.util.Collections.emptyList;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.redis.internal.AutoCloseableLock;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisData;
import org.apache.geode.redis.internal.RedisLockService;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.AbstractExecutor;
import org.apache.geode.redis.internal.executor.RedisHashInRegion;

/**
 * Executor for handling HASH datatypes
 */
public abstract class HashExecutor extends AbstractExecutor {


  protected final int FIELD_INDEX = 2;

  /**
   * Get the save map
   *
   * @param context the context
   * @param key the region hash key region:<key>
   * @return the map data
   */
  protected RedisHash getRedisHash(ExecutionHandlerContext context,
      ByteArrayWrapper key) {
    Region<ByteArrayWrapper, RedisData> region =
        context.getRegionProvider().getDataRegion();

    RedisData data = region.get(key);
    if (data == null) {
      return RedisHash.EMPTY;
    }
    return RedisHashInRegion.checkType(data);
  }

  protected RedisHash getModifiableRedisHash(ExecutionHandlerContext context,
      ByteArrayWrapper key) {
    Region<ByteArrayWrapper, RedisData> region =
        context.getRegionProvider().getDataRegion();

    RedisData data = region.get(key);
    if (data == null) {
      return new RedisHash(emptyList());
    }
    return RedisHashInRegion.checkType(data);
  }

  protected AutoCloseableLock withRegionLock(ExecutionHandlerContext context, ByteArrayWrapper key)
      throws InterruptedException, TimeoutException {
    RedisLockService lockService = context.getLockService();

    return lockService.lock(key);
  }


  /**
   * Save the redisHash information to a region
   *
   * @param redisHash the redisHash to save
   * @param context the execution handler context
   * @param key the raw HASH key
   */
  protected void saveRedishHash(RedisHash redisHash,
      ExecutionHandlerContext context,
      ByteArrayWrapper key) {

    if (redisHash == null) {
      return;
    }

    RegionProvider rp = context.getRegionProvider();

    rp.getDataRegion().put(key, redisHash);
  }
}
