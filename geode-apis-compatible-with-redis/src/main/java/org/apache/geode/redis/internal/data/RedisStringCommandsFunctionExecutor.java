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

package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_STRING;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.CacheTransactionManager;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.string.RedisStringCommands;
import org.apache.geode.redis.internal.executor.string.SetOptions;

public class RedisStringCommandsFunctionExecutor extends RedisDataCommandsFunctionExecutor
    implements
    RedisStringCommands {

  public RedisStringCommandsFunctionExecutor(RegionProvider regionProvider,
      CacheTransactionManager txManager) {
    super(regionProvider, txManager);
  }

  private RedisString getRedisString(RedisKey key, boolean updateStats) {
    return getRegionProvider().getTypedRedisData(RedisDataType.REDIS_STRING, key, updateStats);
  }

  private RedisString getRedisStringIgnoringType(RedisKey key, boolean updateStats) {
    return getRegionProvider().getRedisStringIgnoringType(key, updateStats);
  }

  @Override
  public long append(RedisKey key, byte[] valueToAppend) {
    return stripedExecute(key,
        () -> getRedisString(key, false).append(getRegion(), key, valueToAppend));
  }

  @Override
  public byte[] get(RedisKey key) {
    return stripedExecute(key, () -> getRedisString(key, true).get());
  }

  @Override
  public byte[] mget(RedisKey key) {
    return stripedExecute(key, () -> getRedisStringIgnoringType(key, true).get());
  }

  @Override
  public boolean set(RedisKey key, byte[] value, SetOptions options) {
    return stripedExecute(key, () -> set(getRegionProvider(), key, value, options));
  }

  @Override
  public byte[] incr(RedisKey key) {
    return stripedExecute(key, () -> getRedisString(key, false).incr(getRegion(), key));
  }

  @Override
  public byte[] decr(RedisKey key) {
    return stripedExecute(key, () -> getRedisString(key, false).decr(getRegion(), key));
  }

  @Override
  public byte[] getset(RedisKey key, byte[] value) {
    return stripedExecute(key, () -> getRedisString(key, true).getset(getRegion(), key, value));
  }

  @Override
  public byte[] incrby(RedisKey key, long increment) {
    return stripedExecute(key,
        () -> getRedisString(key, false).incrby(getRegion(), key, increment));
  }

  @Override
  public BigDecimal incrbyfloat(RedisKey key, BigDecimal increment) {
    return stripedExecute(key,
        () -> getRedisString(key, false).incrbyfloat(getRegion(), key, increment));
  }

  @Override
  public int bitop(String operation, RedisKey key, List<RedisKey> sources) {
    return bitop(getRegionProvider(), operation, key, sources);
  }

  @Override
  public byte[] decrby(RedisKey key, long decrement) {
    return stripedExecute(key,
        () -> getRedisString(key, false).decrby(getRegion(), key, decrement));
  }

  @Override
  public byte[] getrange(RedisKey key, long start, long end) {
    return stripedExecute(key, () -> getRedisString(key, true).getrange(start, end));
  }

  @Override
  public int setrange(RedisKey key, int offset, byte[] value) {
    return stripedExecute(key,
        () -> getRedisString(key, false).setrange(getRegion(), key, offset, value));
  }

  @Override
  public int bitpos(RedisKey key, int bit, int start, Integer end) {
    return stripedExecute(key, () -> getRedisString(key, true).bitpos(bit, start, end));
  }

  @Override
  public long bitcount(RedisKey key, int start, int end) {
    return stripedExecute(key, () -> getRedisString(key, true).bitcount(start, end));
  }

  @Override
  public long bitcount(RedisKey key) {
    return stripedExecute(key, () -> getRedisString(key, true).bitcount());
  }

  @Override
  public int strlen(RedisKey key) {
    return stripedExecute(key, () -> getRedisString(key, true).strlen());
  }

  @Override
  public int getbit(RedisKey key, int offset) {
    return stripedExecute(key, () -> getRedisString(key, true).getbit(offset));
  }

  @Override
  public int setbit(RedisKey key, long offset, int value) {
    int byteIndex = (int) (offset / 8);
    byte bitIndex = (byte) (offset % 8);
    return stripedExecute(key,
        () -> getRedisString(key, false).setbit(getRegion(), key, value, byteIndex, bitIndex));
  }

  @Override
  public Void mset(List<RedisKey> keys, List<byte[]> values) {
    List<RedisKey> keysToLock = new ArrayList<>(keys.size());
    for (RedisKey key : keys) {
      getRegionProvider().ensureKeyIsLocal(key);
      keysToLock.add(key);
    }

    // Pass a key in so that the bucket will be locked. Since all keys are already guaranteed to be
    // in the same bucket we can use any key for this.
    return stripedExecute(keysToLock.get(0), keysToLock, () -> mset0(keys, values));
  }

  private Void mset0(List<RedisKey> keys, List<byte[]> values) {
    CacheTransactionManager txManager = getTransactionManager();
    try {
      txManager.begin();
      for (int i = 0; i < keys.size(); i++) {
        setRedisString(getRegionProvider(), keys.get(i), values.get(i));
      }
      txManager.commit();
    } finally {
      if (txManager.exists()) {
        txManager.rollback();
      }
    }
    return null;
  }

  private boolean set(RegionProvider regionProvider, RedisKey key, byte[] value,
      SetOptions options) {
    if (options != null) {
      if (options.isNX()) {
        return setnx(regionProvider, key, value, options);
      }

      if (options.isXX() && regionProvider.getRedisData(key).isNull()) {
        return false;
      }
    }

    RedisString redisString = setRedisString(regionProvider, key, value);
    redisString.handleSetExpiration(options);
    return true;
  }

  private boolean setnx(RegionProvider regionProvider, RedisKey key, byte[] value,
      SetOptions options) {
    if (regionProvider.getRedisData(key).exists()) {
      return false;
    }
    RedisString redisString = new RedisString(value);
    redisString.handleSetExpiration(options);
    regionProvider.getDataRegion().put(key, redisString);
    return true;
  }

  private int bitop(RegionProvider regionProvider, String operation, RedisKey key,
      List<RedisKey> sources) {
    List<byte[]> sourceValues = new ArrayList<>();
    int selfIndex = -1;
    // Read all the source values, except for self, before locking the stripe.
    for (RedisKey sourceKey : sources) {
      if (sourceKey.equals(key)) {
        // get self later after the stripe is locked
        selfIndex = sourceValues.size();
        sourceValues.add(null);
      } else {
        sourceValues.add(regionProvider.getStringCommands().get(sourceKey));
      }
    }
    int indexOfSelf = selfIndex;
    return regionProvider.execute(key,
        () -> doBitOp(regionProvider, operation, key, indexOfSelf, sourceValues));
  }

  private enum BitOp {
    AND, OR, XOR
  }

  private int doBitOp(RegionProvider regionProvider, String operation, RedisKey key, int selfIndex,
      List<byte[]> sourceValues) {
    if (selfIndex != -1) {
      RedisString redisString =
          regionProvider.getTypedRedisData(RedisDataType.REDIS_STRING, key, true);
      if (!redisString.isNull()) {
        sourceValues.set(selfIndex, redisString.getValue());
      }
    }
    int maxLength = 0;
    for (byte[] sourceValue : sourceValues) {
      if (sourceValue != null && maxLength < sourceValue.length) {
        maxLength = sourceValue.length;
      }
    }
    byte[] newValue;
    switch (operation) {
      case "AND":
        newValue = doBitOp(BitOp.AND, sourceValues, maxLength);
        break;
      case "OR":
        newValue = doBitOp(BitOp.OR, sourceValues, maxLength);
        break;
      case "XOR":
        newValue = doBitOp(BitOp.XOR, sourceValues, maxLength);
        break;
      default: // NOT
        newValue = not(sourceValues.get(0), maxLength);
        break;
    }
    if (newValue.length == 0) {
      regionProvider.getDataRegion().remove(key);
    } else {
      setRedisString(regionProvider, key, newValue);
    }
    return newValue.length;
  }

  private byte[] doBitOp(BitOp bitOp, List<byte[]> sourceValues, int max) {
    byte[] dest = new byte[max];
    for (int i = 0; i < max; i++) {
      byte b = 0;
      boolean firstByte = true;
      for (byte[] sourceValue : sourceValues) {
        byte sourceByte = 0;
        if (sourceValue != null && i < sourceValue.length) {
          sourceByte = sourceValue[i];
        }
        if (firstByte) {
          b = sourceByte;
          firstByte = false;
        } else {
          switch (bitOp) {
            case AND:
              b &= sourceByte;
              break;
            case OR:
              b |= sourceByte;
              break;
            case XOR:
              b ^= sourceByte;
              break;
          }
        }
      }
      dest[i] = b;
    }
    return dest;
  }

  private byte[] not(byte[] sourceValue, int max) {
    byte[] dest = new byte[max];
    if (sourceValue == null) {
      for (int i = 0; i < max; i++) {
        dest[i] = ~0;
      }
    } else {
      for (int i = 0; i < max; i++) {
        dest[i] = (byte) (~sourceValue[i] & 0xFF);
      }
    }
    return dest;
  }

  private RedisString setRedisString(RegionProvider regionProvider, RedisKey key, byte[] value) {
    RedisString result;
    RedisData redisData = regionProvider.getRedisData(key);

    if (redisData.isNull() || redisData.getType() != REDIS_STRING) {
      result = new RedisString(value);
    } else {
      result = (RedisString) redisData;
      result.set(value);
    }
    regionProvider.getDataRegion().put(key, result);
    return result;
  }
}
