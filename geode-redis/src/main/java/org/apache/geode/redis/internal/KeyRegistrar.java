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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.geode.DataSerializer;
import org.apache.geode.InvalidDeltaException;
import org.apache.geode.cache.Region;

public class KeyRegistrar {
  private Region<ByteArrayWrapper, RedisData> redisDataRegion;

  public KeyRegistrar(Region<ByteArrayWrapper, RedisData> redisDataRegion) {
    this.redisDataRegion = redisDataRegion;
  }

  /**
   * Checks if the givenKey is associated with the passed data type. If an entry doesn't exist,
   * store the key:datatype association in the metadataRegion
   */
  public void register(ByteArrayWrapper key, RedisDataType type) {
    RedisData existingValue = this.redisDataRegion.putIfAbsent(key, transformType(type));
    if (!isValidDataType(existingValue, type)) {
      throwDataTypeException(key, existingValue);
    }
  }

  /**
   * TODO: This class should go away once all data types implement RedisData.
   */
  private static class RedisDataTransformer implements RedisData {
    private RedisDataType type;

    public RedisDataTransformer(RedisDataType type) {
      this.type = type;
    }

    public RedisDataTransformer() {
      // needed for serialization
    }

    @Override
    public RedisDataType getType() {
      return type;
    }

    @Override
    public void toData(DataOutput out) throws IOException {
      DataSerializer.writeEnum(type, out);
    }

    @Override
    public void fromData(DataInput in) throws IOException, ClassNotFoundException {
      type = DataSerializer.readEnum(RedisDataType.class, in);
    }

    @Override
    public boolean hasDelta() {
      return false;
    }

    @Override
    public void toDelta(DataOutput out) throws IOException {}

    @Override
    public void fromDelta(DataInput in) throws IOException, InvalidDeltaException {}
  }

  private static final RedisDataTransformer REDIS_SORTEDSET_DATA =
      new RedisDataTransformer(RedisDataType.REDIS_SORTEDSET);
  private static final RedisDataTransformer REDIS_LIST_DATA =
      new RedisDataTransformer(RedisDataType.REDIS_LIST);
  private static final RedisDataTransformer REDIS_STRING_DATA =
      new RedisDataTransformer(RedisDataType.REDIS_STRING);
  private static final RedisDataTransformer REDIS_PROTECTED_DATA =
      new RedisDataTransformer(RedisDataType.REDIS_PROTECTED);
  private static final RedisDataTransformer REDIS_HLL_DATA =
      new RedisDataTransformer(RedisDataType.REDIS_HLL);
  private static final RedisDataTransformer REDIS_PUBSUB_DATA =
      new RedisDataTransformer(RedisDataType.REDIS_PUBSUB);

  /**
   * TODO: This method should only exist while we still have data types that have not implemented
   * this RedisData interface. Once they all implement it then we can get rid of this.
   */
  private RedisData transformType(RedisDataType type) {
    switch (type) {
      case REDIS_SORTEDSET:
        return REDIS_SORTEDSET_DATA;
      case REDIS_LIST:
        return REDIS_LIST_DATA;
      case REDIS_STRING:
        return REDIS_STRING_DATA;
      case REDIS_PROTECTED:
        return REDIS_PROTECTED_DATA;
      case REDIS_HLL:
        return REDIS_HLL_DATA;
      case REDIS_PUBSUB:
        return REDIS_PUBSUB_DATA;
      case REDIS_HASH:
      case REDIS_SET:
        throw new IllegalStateException(
            type + " should never be added as a type to the data region");
      default:
        throw new IllegalStateException("unexpected RedisDataType: " + type);
    }
  }

  public boolean unregister(ByteArrayWrapper key) {
    return this.redisDataRegion.remove(key) != null;
  }

  public boolean isRegistered(ByteArrayWrapper key) {
    return this.redisDataRegion.containsKey(key);
  }

  public Set<ByteArrayWrapper> keys() {
    return this.redisDataRegion.keySet();
  }

  public Set<Map.Entry<ByteArrayWrapper, RedisData>> keyInfos() {
    return this.redisDataRegion.entrySet();
  }

  public int numKeys() {
    return this.redisDataRegion.size() - GeodeRedisServer.PROTECTED_KEY_COUNT;
  }

  public RedisDataType getType(ByteArrayWrapper key) {
    RedisData currentValue = redisDataRegion.get(key);
    if (currentValue == null) {
      return null;
    }
    return currentValue.getType();
  }

  /**
   * Checks if the given key is associated with the passed data type. If there is a mismatch, a
   * {@link RuntimeException} is thrown
   *
   * @param key Key to check
   * @param type Type to check to
   */
  public void validate(ByteArrayWrapper key, RedisDataType type) {
    RedisData currentValue = redisDataRegion.get(key);
    if (currentValue != null) {
      RedisDataType currentType = currentValue.getType();
      if (!isValidDataType(currentType, type)) {
        throwDataTypeException(key, currentType);
      }
    }
  }

  /**
   * Checks if the given key is a protected string in GeodeRedis
   *
   * @param key Key to check
   */
  public boolean isProtected(ByteArrayWrapper key) {
    RedisData redisData = redisDataRegion.get(key);
    if (redisData == null) {
      return false;
    }
    return RedisDataType.REDIS_PROTECTED.equals(redisData.getType());
  }

  private boolean isValidDataType(RedisData actualData, RedisDataType expectedDataType) {
    if (actualData == null) {
      return true;
    }
    return isValidDataType(actualData.getType(), expectedDataType);
  }

  private boolean isValidDataType(RedisDataType actualDataType, RedisDataType expectedDataType) {
    return isKeyUnused(actualDataType) || actualDataType.equals(expectedDataType);
  }

  private boolean isKeyUnused(RedisDataType dataType) {
    return dataType == null;
  }

  private void throwDataTypeException(ByteArrayWrapper key, RedisData data) {
    throwDataTypeException(key, data.getType());
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
