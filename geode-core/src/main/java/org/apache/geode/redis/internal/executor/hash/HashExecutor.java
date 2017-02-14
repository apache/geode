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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public abstract class HashExecutor extends AbstractExecutor {

  protected final int FIELD_INDEX = 2;

  @SuppressWarnings("unchecked")
  protected Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> getOrCreateRegion(
      ExecutionHandlerContext context, ByteArrayWrapper key, RedisDataType type) {

    key = HashUtil.toRegionNameByteArray(key);

    return (Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>>) context
        .getRegionProvider().getOrCreateRegion(key, type, context);
  }

  /**
   * 
   * @param context the context
   * @param key the region hash key region:<key>
   * @return return getMap(context,key,RedisDataType.REDIS_HASH)
   */
  protected Map<ByteArrayWrapper, ByteArrayWrapper> getMap(ExecutionHandlerContext context,
      ByteArrayWrapper key) {
    return getMap(context, key, RedisDataType.REDIS_HASH);
  }

  /**
   * 
   * @param context the context
   * @param key the region hash key region:<key>
   * @param type the command type
   * @return the map data
   */
  protected Map<ByteArrayWrapper, ByteArrayWrapper> getMap(ExecutionHandlerContext context,
      ByteArrayWrapper key, RedisDataType type) {

    ByteArrayWrapper regionName = HashUtil.toRegionNameByteArray(key);

    Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> region =
        getOrCreateRegion(context, regionName, type);

    if (region == null)
      return null;

    ByteArrayWrapper entryKey = HashUtil.toEntryKey(key);
    Map<ByteArrayWrapper, ByteArrayWrapper> map = region.get(entryKey);
    if (map == null) {
      map = new ConcurrentHashMap<ByteArrayWrapper, ByteArrayWrapper>();
    }

    return map;

  }

  @SuppressWarnings("unchecked")
  protected Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> getRegion(
      ExecutionHandlerContext context, ByteArrayWrapper key) {
    return (Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>>) context
        .getRegionProvider().getRegion(key);
  }

  /**
   * 
   * @param map the map to save
   * @param context the execution handler context
   * @param key the raw HASH key
   * @param type the redis data type
   */
  protected void saveMap(Map<ByteArrayWrapper, ByteArrayWrapper> map,
      ExecutionHandlerContext context, ByteArrayWrapper key) {

    saveMap(map, context, key, RedisDataType.REDIS_HASH);
  }

  /**
   * 
   * @param map the map to save
   * @param context the execution handler context
   * @param key the raw HASH key
   * @param type the redis data type
   */
  protected void saveMap(Map<ByteArrayWrapper, ByteArrayWrapper> map,
      ExecutionHandlerContext context, ByteArrayWrapper key, RedisDataType type) {

    if (map == null) {
      return;
    }

    ByteArrayWrapper regionName = HashUtil.toRegionNameByteArray(key);

    Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> region =
        getOrCreateRegion(context, regionName, type);

    ByteArrayWrapper entryKey = HashUtil.toEntryKey(key);

    region.put(entryKey, map);

  }


}
