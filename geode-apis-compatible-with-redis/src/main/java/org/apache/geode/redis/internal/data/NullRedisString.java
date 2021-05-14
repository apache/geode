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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.RegionProvider;
import org.apache.geode.redis.internal.executor.string.SetOptions;
import org.apache.geode.redis.internal.netty.Coder;

/**
 * Used when a RedisString instance does not exist.
 * The code that looks for an existing instance will
 * return this one, instead of null, when it does not
 * find an existing instance.
 * A canonical instance of this class will be used so
 * care must be taken that it does not allow itself
 * to be modified.
 */
public class NullRedisString extends RedisString {
  public NullRedisString() {
    super(new byte[0]);
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  protected void valueAppend(byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void valueSet(byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public byte[] get() {
    return null;
  }

  @Override
  public int bitpos(int bit, int start, Integer end) {
    if (bit == 0) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public int setbit(Region<RedisKey, RedisData> region, RedisKey key,
      int bitValue, int byteIndex, byte bitIndex) {
    RedisString newValue;
    if (bitValue == 1) {
      byte[] bytes = new byte[byteIndex + 1];
      bytes[byteIndex] = (byte) (0x80 >> bitIndex);
      newValue = new RedisString(bytes);
    } else {
      // all bits are 0 so use an empty byte array
      newValue = new RedisString(new byte[0]);
    }
    region.put(key, newValue);
    return 0;
  }

  @Override
  public long incr(Region<RedisKey, RedisData> region, RedisKey key)
      throws NumberFormatException, ArithmeticException {
    byte[] newValue = {Coder.NUMBER_1_BYTE};
    region.put(key, new RedisString(newValue));
    return 1;
  }

  @Override
  public long incrby(Region<RedisKey, RedisData> region, RedisKey key,
      long increment) throws NumberFormatException, ArithmeticException {
    byte[] newValue = Coder.longToBytes(increment);
    region.put(key, new RedisString(newValue));
    return increment;
  }

  @Override
  public BigDecimal incrbyfloat(Region<RedisKey, RedisData> region, RedisKey key,
      BigDecimal increment) throws NumberFormatException, ArithmeticException {
    byte[] newValue = Coder.bigDecimalToBytes(increment);
    region.put(key, new RedisString(newValue));
    return increment;
  }

  @Override
  public long decr(Region<RedisKey, RedisData> region, RedisKey key)
      throws NumberFormatException, ArithmeticException {
    region.put(key, new RedisString(Coder.stringToBytes("-1")));
    return -1;
  }

  @Override
  public long decrby(Region<RedisKey, RedisData> region, RedisKey key, long decrement) {
    byte[] newValue = Coder.longToBytes(-decrement);
    region.put(key, new RedisString(newValue));
    return -decrement;
  }

  @Override
  public int append(Region<RedisKey, RedisData> region, RedisKey key, byte[] appendValue) {
    region.put(key, new RedisString(appendValue));
    return appendValue.length;
  }

  @Override
  public byte[] getset(Region<RedisKey, RedisData> region, RedisKey key, byte[] value) {
    region.put(key, new RedisString(value));
    return null;
  }

  @Override
  public int setrange(Region<RedisKey, RedisData> region, RedisKey key, int offset,
      byte[] valueToAdd) {
    byte[] newBytes = valueToAdd;
    if (valueToAdd.length != 0) {
      if (offset != 0) {
        newBytes = new byte[offset + valueToAdd.length];
        System.arraycopy(valueToAdd, 0, newBytes, offset, valueToAdd.length);
      }
      region.put(key, new RedisString(newBytes));
    }
    return newBytes.length;
  }

  /**
   * SET is currently mostly implemented here. It does not have an implementation on
   * RedisString which is a bit odd.
   */
  public boolean set(RegionProvider regionProvider, RedisKey key, byte[] value,
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

  /**
   * bitop is currently only implemented here. It does not have an implementation on
   * RedisString which is a bit odd. This implementation only has a couple of places
   * that care if a RedisString for "key" exists.
   */
  public int bitop(RegionProvider regionProvider, String operation, RedisKey key,
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

  public RedisString setRedisString(RegionProvider regionProvider, RedisKey key, byte[] value) {
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
