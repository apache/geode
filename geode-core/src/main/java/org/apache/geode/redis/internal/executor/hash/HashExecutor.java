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

import java.util.HashMap;
import java.util.Map;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public abstract class HashExecutor extends AbstractExecutor {
  /**
   * <pre>
   * The region:key separator.
   * 
   *  REGION_KEY_SEPERATOR = ":"
   * 
   * See Hash section of <a href=
  "https://redis.io/topics/data-types">https://redis.io/topics/data-types#Hashes</a>
   * </pre>
   */
  public static final String REGION_KEY_SEPERATOR = ":";

  /**
   * The default hash region name REGION_HASH_REGION = Coder.stringToByteArrayWrapper("ReDiS_HASH")
   */
  public static final ByteArrayWrapper REGION_HASH_REGION =
      Coder.stringToByteArrayWrapper(GeodeRedisServer.HASH_REGION);

  protected final int FIELD_INDEX = 2;

  protected Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> getOrCreateRegion(
      ExecutionHandlerContext context, ByteArrayWrapper key, RedisDataType type) {

    return getRegion(context, key);
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
   * Get the save map
   * 
   * @param context the context
   * @param key the region hash key region:<key>
   * @param type the command type
   * @return the map data
   */
  protected Map<ByteArrayWrapper, ByteArrayWrapper> getMap(ExecutionHandlerContext context,
      ByteArrayWrapper key, RedisDataType type) {

    ByteArrayWrapper regionName = toRegionNameByteArray(key);

    Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> region =
        getOrCreateRegion(context, regionName, type);

    if (region == null)
      return null;


    ByteArrayWrapper entryKey = toEntryKey(key);
    Map<ByteArrayWrapper, ByteArrayWrapper> map = region.get(entryKey);
    if (map == null) {
      map = new HashMap<ByteArrayWrapper, ByteArrayWrapper>();
    }

    return map;

  }

  /**
   * Return key from format region:key
   * 
   * @param key the raw key
   * @return the ByteArray for the key
   */
  public static ByteArrayWrapper toEntryKey(ByteArrayWrapper key) {
    if (key == null)
      return null;

    String keyString = key.toString();
    int nameSeparatorIndex = keyString.indexOf(REGION_KEY_SEPERATOR);
    if (nameSeparatorIndex < 0) {
      return key;
    }

    keyString = keyString.substring(nameSeparatorIndex + 1);
    key = new ByteArrayWrapper(Coder.stringToBytes(keyString));
    return key;
  }

  /**
   * 
   * @param context the execution handler context
   * @param key the raw command key
   * @return region determine based on the raw command key
   */
  protected Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> getRegion(
      ExecutionHandlerContext context, ByteArrayWrapper key) {
    if (key == null)
      return null;

    if (key.toString().contains(REGION_KEY_SEPERATOR)) {
      return (Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>>) context
          .getRegionProvider()
          .getOrCreateRegion(toRegionNameByteArray(key), RedisDataType.REDIS_HASH, context);
    }

    // default region
    return context.getRegionProvider().getHashRegion();
  }

  /**
   * Supports conversation of keys with format region:key
   * 
   * @param key the byte array wrapper
   * @return the byte array wrapper
   */
  public static ByteArrayWrapper toRegionNameByteArray(ByteArrayWrapper key) {
    if (key == null)
      return null;

    String keyString = key.toString();

    int nameSeparatorIndex = keyString.indexOf(REGION_KEY_SEPERATOR);
    if (nameSeparatorIndex > -1) {
      keyString = keyString.substring(0, nameSeparatorIndex);
      if (keyString == null)
        return REGION_HASH_REGION;

      keyString = keyString.trim();

      if (keyString.length() == 0)
        return REGION_HASH_REGION;

      key = new ByteArrayWrapper(Coder.stringToBytes(keyString.trim()));

      return key;
    }

    return REGION_HASH_REGION;
  }

  /**
   * Save the map to the region
   * 
   * @param map the map to save
   * @param context the execution handler context
   * @param key the raw HASH key
   */
  protected void saveMap(Map<ByteArrayWrapper, ByteArrayWrapper> map,
      ExecutionHandlerContext context, ByteArrayWrapper key) {

    saveMap(map, context, key, RedisDataType.REDIS_HASH);
  }

  /**
   * Save the map information to a region
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

    ByteArrayWrapper regionName = toRegionNameByteArray(key);


    Region<ByteArrayWrapper, Map<ByteArrayWrapper, ByteArrayWrapper>> region =
        getOrCreateRegion(context, regionName, type);

    ByteArrayWrapper entryKey = toEntryKey(key);

    region.put(entryKey, map);
    context.getRegionProvider().metaPut(key, RedisDataType.REDIS_HASH);

  }


}
