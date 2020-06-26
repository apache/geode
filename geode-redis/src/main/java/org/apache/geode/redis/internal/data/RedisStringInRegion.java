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
 *
 */
package org.apache.geode.redis.internal.data;

import static org.apache.geode.redis.internal.data.RedisDataType.REDIS_STRING;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisStats;
import org.apache.geode.redis.internal.executor.StripedExecutor;
import org.apache.geode.redis.internal.executor.string.RedisStringCommands;
import org.apache.geode.redis.internal.executor.string.RedisStringCommandsFunctionExecutor;
import org.apache.geode.redis.internal.executor.string.SetOptions;
import org.apache.geode.redis.internal.netty.Coder;

public class RedisStringInRegion extends RedisKeyInRegion implements RedisStringCommands {

  public RedisStringInRegion(Region<ByteArrayWrapper, RedisData> region,
      RedisStats redisStats) {
    super(region, redisStats);
  }

  @Override
  public long append(ByteArrayWrapper key, ByteArrayWrapper valueToAppend) {
    RedisString redisString = getRedisString(key);

    if (redisString != null) {
      return redisString.append(valueToAppend, region, key);
    } else {
      region.put(key, new RedisString(valueToAppend));
      return valueToAppend.length();
    }
  }

  @Override
  public ByteArrayWrapper get(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString != null) {
      return redisString.get();
    } else {
      return null;
    }
  }

  @Override
  public ByteArrayWrapper mget(ByteArrayWrapper key) {
    // like get but does not do a type check
    RedisData redisData = getRedisData(key);
    if (redisData instanceof RedisString) {
      RedisString redisString = (RedisString) redisData;
      return redisString.get();
    }
    return null;
  }

  @Override
  public boolean set(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    if (options != null) {
      if (options.isNX()) {
        return setnx(key, value, options);
      }

      if (options.isXX() && getRedisData(key) == null) {
        return false;
      }
    }

    RedisString redisString = getRedisStringForSet(key);
    if (redisString == null) {
      redisString = new RedisString(value);
    } else {
      redisString.set(value);
    }
    handleSetExpiration(redisString, options);
    region.put(key, redisString);
    return true;
  }

  @Override
  public long incr(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = {Coder.NUMBER_1_BYTE};
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return 1;
    }

    return redisString.incr(region, key);
  }

  @Override
  public long decr(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      redisString = new RedisString(new ByteArrayWrapper(Coder.stringToBytes("-1")));
      region.put(key, redisString);
      return -1;
    }

    return redisString.decr(region, key);
  }

  @Override
  public ByteArrayWrapper getset(ByteArrayWrapper key, ByteArrayWrapper value) {
    ByteArrayWrapper result = null;
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      region.put(key, new RedisString(value));
    } else {
      result = redisString.get();
      redisString.set(value);
      redisString.persistNoDelta();
      region.put(key, redisString);
    }
    return result;
  }

  @Override
  public long incrby(ByteArrayWrapper key, long increment) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = Coder.longToBytes(increment);
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return increment;
    }

    return redisString.incrby(region, key, increment);
  }

  @Override
  public double incrbyfloat(ByteArrayWrapper key, double increment) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = Coder.doubleToBytes(increment);
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return increment;
    }

    return redisString.incrbyfloat(region, key, increment);
  }

  @SuppressWarnings("unchecked")
  public int bitop(StripedExecutor stripedExecutor, String operation, ByteArrayWrapper key,
      List<ByteArrayWrapper> sources) {
    List<ByteArrayWrapper> sourceValues = new ArrayList<>();
    int selfIndex = -1;
    // Read all the source values, except for self, before locking the stripe.
    Region fetchRegion = region;
    if (fetchRegion instanceof LocalDataSet) {
      LocalDataSet lds = (LocalDataSet) fetchRegion;
      fetchRegion = lds.getProxy();
    }
    RedisStringCommands commander = new RedisStringCommandsFunctionExecutor(fetchRegion);
    for (ByteArrayWrapper sourceKey : sources) {
      if (sourceKey.equals(key)) {
        // get self later after the stripe is locked
        selfIndex = sourceValues.size();
        sourceValues.add(null);
      } else {
        sourceValues.add(commander.get(sourceKey));
      }
    }
    int indexOfSelf = selfIndex;
    return stripedExecutor.execute(key, () -> doBitOp(operation, key, indexOfSelf, sourceValues));
  }

  private int doBitOp(String operation, ByteArrayWrapper key, int selfIndex,
      List<ByteArrayWrapper> sourceValues) {
    if (selfIndex != -1) {
      RedisString redisString = getRedisString(key);
      if (redisString != null) {
        sourceValues.set(selfIndex, redisString.getValue());
      }
    }
    int maxLength = 0;
    for (ByteArrayWrapper sourceValue : sourceValues) {
      if (sourceValue != null && maxLength < sourceValue.length()) {
        maxLength = sourceValue.length();
      }
    }
    ByteArrayWrapper newValue;
    switch (operation) {
      case "AND":
        newValue = and(sourceValues, maxLength);
        break;
      case "OR":
        newValue = or(sourceValues, maxLength);
        break;
      case "XOR":
        newValue = xor(sourceValues, maxLength);
        break;
      default: // NOT
        newValue = not(sourceValues.get(0), maxLength);
        break;
    }
    if (newValue.length() == 0) {
      region.remove(key);
    } else {
      RedisString redisString = getRedisStringForSet(key);
      if (redisString == null) {
        redisString = new RedisString(newValue);
      } else {
        redisString.set(newValue);
      }
      region.put(key, redisString);
    }
    return newValue.length();
  }

  private ByteArrayWrapper and(List<ByteArrayWrapper> sourceValues, int max) {
    byte[] dest = new byte[max];
    for (int i = 0; i < max; i++) {
      byte b = 0;
      boolean firstByte = true;
      for (ByteArrayWrapper sourceValue : sourceValues) {
        byte sourceByte = 0;
        if (sourceValue != null && i < sourceValue.length()) {
          sourceByte = sourceValue.toBytes()[i];
        }
        if (firstByte) {
          b = sourceByte;
          firstByte = false;
        } else {
          b &= sourceByte;
        }
      }
      dest[i] = b;
    }
    return new ByteArrayWrapper(dest);
  }

  private ByteArrayWrapper or(List<ByteArrayWrapper> sourceValues, int max) {
    byte[] dest = new byte[max];
    for (int i = 0; i < max; i++) {
      byte b = 0;
      boolean firstByte = true;
      for (ByteArrayWrapper sourceValue : sourceValues) {
        byte sourceByte = 0;
        if (sourceValue != null && i < sourceValue.length()) {
          sourceByte = sourceValue.toBytes()[i];
        }
        if (firstByte) {
          b = sourceByte;
          firstByte = false;
        } else {
          b |= sourceByte;
        }
      }
      dest[i] = b;
    }
    return new ByteArrayWrapper(dest);
  }

  private ByteArrayWrapper xor(List<ByteArrayWrapper> sourceValues, int max) {
    byte[] dest = new byte[max];
    for (int i = 0; i < max; i++) {
      byte b = 0;
      boolean firstByte = true;
      for (ByteArrayWrapper sourceValue : sourceValues) {
        byte sourceByte = 0;
        if (sourceValue != null && i < sourceValue.length()) {
          sourceByte = sourceValue.toBytes()[i];
        }
        if (firstByte) {
          b = sourceByte;
          firstByte = false;
        } else {
          b ^= sourceByte;
        }
      }
      dest[i] = b;
    }
    return new ByteArrayWrapper(dest);
  }

  private ByteArrayWrapper not(ByteArrayWrapper sourceValue, int max) {
    byte[] dest = new byte[max];
    if (sourceValue == null) {
      for (int i = 0; i < max; i++) {
        dest[i] = ~0;
      }
    } else {
      byte[] cA = sourceValue.toBytes();
      for (int i = 0; i < max; i++) {
        dest[i] = (byte) (~cA[i] & 0xFF);
      }
    }
    return new ByteArrayWrapper(dest);
  }

  @Override
  public int bitop(String operation, ByteArrayWrapper destKey, List<ByteArrayWrapper> sources) {
    throw new IllegalStateException("should never be called");
  }

  @Override
  public long decrby(ByteArrayWrapper key, long decrement) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newValue = Coder.longToBytes(-decrement);
      redisString = new RedisString(new ByteArrayWrapper(newValue));
      region.put(key, redisString);
      return -decrement;
    }

    return redisString.decrby(region, key, decrement);
  }

  @Override
  public ByteArrayWrapper getrange(ByteArrayWrapper key, long start, long end) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      return new ByteArrayWrapper(new byte[0]);
    }
    return redisString.getrange(start, end);
  }

  @Override
  public int setrange(ByteArrayWrapper key, int offset, byte[] value) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      byte[] newBytes = value;
      if (value.length != 0) {
        if (offset != 0) {
          newBytes = new byte[offset + value.length];
          System.arraycopy(value, 0, newBytes, offset, value.length);
        }
        redisString = new RedisString(new ByteArrayWrapper(newBytes));
        region.put(key, redisString);
      }
      return newBytes.length;
    }
    return redisString.setrange(region, key, offset, value);
  }

  @Override
  public int bitpos(ByteArrayWrapper key, int bit, int start, Integer end) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      if (bit == 0) {
        return 0;
      } else {
        return -1;
      }
    }
    return redisString.bitpos(region, key, bit, start, end);
  }

  @Override
  public long bitcount(ByteArrayWrapper key, int start, int end) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      return 0;
    }
    return redisString.bitcount(start, end);
  }

  @Override
  public long bitcount(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      return 0;
    }
    return redisString.bitcount();
  }

  @Override
  public int strlen(ByteArrayWrapper key) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      return 0;
    }
    return redisString.strlen();
  }

  @Override
  public int getbit(ByteArrayWrapper key, int offset) {
    RedisString redisString = getRedisString(key);

    if (redisString == null) {
      return 0;
    }
    return redisString.getbit(offset);
  }

  @Override
  public int setbit(ByteArrayWrapper key, long offset, int value) {
    RedisString redisString = getRedisString(key);
    int byteIndex = (int) (offset / 8);
    byte bitIndex = (byte) (offset % 8);

    if (redisString == null) {
      RedisString newValue;
      if (value == 1) {
        byte[] bytes = new byte[byteIndex + 1];
        bytes[byteIndex] = (byte) (0x80 >> bitIndex);
        newValue = new RedisString(new ByteArrayWrapper(bytes));
      } else {
        // all bits are 0 so use an empty byte array
        newValue = new RedisString(new ByteArrayWrapper(new byte[0]));
      }
      region.put(key, newValue);
      return 0;
    }
    return redisString.setbit(region, key, value, byteIndex, bitIndex);
  }

  private boolean setnx(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    if (getRedisData(key) != null) {
      return false;
    }
    RedisString redisString = new RedisString(value);
    handleSetExpiration(redisString, options);
    region.put(key, redisString);
    return true;
  }

  private void handleSetExpiration(RedisString redisString, SetOptions options) {
    long setExpiration = options == null ? 0L : options.getExpiration();
    if (setExpiration != 0) {
      long now = System.currentTimeMillis();
      long timestamp = now + setExpiration;
      redisString.setExpirationTimestampNoDelta(timestamp);
    } else if (options == null || !options.isKeepTTL()) {
      redisString.persistNoDelta();
    }
  }

  private RedisString checkType(RedisData redisData) {
    if (redisData == null) {
      return null;
    }
    if (redisData.getType() != REDIS_STRING) {
      throw new RedisDataTypeMismatchException(RedisConstants.ERROR_WRONG_TYPE);
    }
    return (RedisString) redisData;
  }

  private RedisString getRedisString(ByteArrayWrapper key) {
    return checkType(getRedisData(key));
  }

  private RedisString getRedisStringForSet(ByteArrayWrapper key) {
    RedisData redisData = getRedisData(key);
    if (redisData == null || redisData.getType() != REDIS_STRING) {
      return null;
    }
    return (RedisString) redisData;
  }
}
