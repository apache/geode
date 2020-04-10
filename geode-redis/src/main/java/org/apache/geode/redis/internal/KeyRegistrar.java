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

import java.util.Map;
import java.util.Set;

import org.apache.geode.cache.Region;

public class KeyRegistrar {
  private Region<String, RedisDataType> redisMetaRegion;

  public KeyRegistrar(Region<String, RedisDataType> redisMetaRegion) {
    this.redisMetaRegion = redisMetaRegion;
  }

  /**
   * Checks if the givenKey is associated with the passed data type. If an entry doesn't exist,
   * store the key:datatype association in the metadataRegion
   */
  public void register(ByteArrayWrapper key, RedisDataType type) {
    RedisDataType existingType = this.redisMetaRegion.putIfAbsent(key.toString(), type);
    if (!isValidDataType(existingType, type)) {
      throwDataTypeException(key, existingType);
    }
  }

  public boolean unregister(ByteArrayWrapper key) {
    return this.redisMetaRegion.remove(key.toString()) != null;
  }

  public boolean isRegistered(ByteArrayWrapper key) {
    return this.redisMetaRegion.containsKey(key.toString());
  }

  public Set<String> keys() {
    Set<String> keysWithProtected = this.redisMetaRegion.keySet();
    return keysWithProtected;
  }

  public Set<Map.Entry<String, RedisDataType>> keyInfos() {
    return this.redisMetaRegion.entrySet();
  }

  public int numKeys() {
    return this.redisMetaRegion.size() - RedisConstants.NUM_DEFAULT_KEYS;
  }

  public RedisDataType getType(ByteArrayWrapper key) {
    return this.redisMetaRegion.get(key.toString());
  }

  /**
   * Checks if the given key is associated with the passed data type. If there is a mismatch, a
   * {@link RuntimeException} is thrown
   *
   * @param key Key to check
   * @param type Type to check to
   */
  public void validate(ByteArrayWrapper key, RedisDataType type) {
    RedisDataType currentType = redisMetaRegion.get(key.toString());
    if (!isValidDataType(currentType, type)) {
      throwDataTypeException(key, currentType);
    }
  }

  /**
   * Checks if the given key is a protected string in GeodeRedis
   *
   * @param key Key to check
   */
  public boolean isProtected(ByteArrayWrapper key) {
    return RedisDataType.REDIS_PROTECTED.equals(redisMetaRegion.get(key.toString()));
  }

  private boolean isValidDataType(RedisDataType actualDataType, RedisDataType expectedDataType) {
    return isKeyUnused(actualDataType) || actualDataType.equals(expectedDataType);
  }

  private boolean isKeyUnused(RedisDataType dataType) {
    return dataType == null;
  }

  private void throwDataTypeException(ByteArrayWrapper key, RedisDataType dataType) {
    if (RedisDataType.REDIS_PROTECTED.equals(dataType)) {
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is protected");
    } else {
      throw new RedisDataTypeMismatchException(
          RedisConstants.ERROR_WRONG_TYPE);
    }
  }
}
