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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.executor.StripedExecutor;
import org.apache.geode.redis.internal.executor.string.RedisStringCommands;
import org.apache.geode.redis.internal.executor.string.RedisStringCommandsFunctionInvoker;
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
    super(new ByteArrayWrapper(new byte[0]));
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
  protected void valueSet(ByteArrayWrapper newValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void valueSetBytes(byte[] bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public ByteArrayWrapper get() {
    return null;
  }

  @Override
  public int bitpos(Region<RedisKey, RedisData> region, RedisKey key, int bit,
      int start, Integer end) {
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
      newValue = new RedisString(new ByteArrayWrapper(bytes));
    } else {
      // all bits are 0 so use an empty byte array
      newValue = new RedisString(new ByteArrayWrapper(new byte[0]));
    }
    region.put(key, newValue);
    return 0;
  }

  @Override
  public long incr(Region<RedisKey, RedisData> region, RedisKey key)
      throws NumberFormatException, ArithmeticException {
    byte[] newValue = {Coder.NUMBER_1_BYTE};
    region.put(key, new RedisString(new ByteArrayWrapper(newValue)));
    return 1;
  }

  @Override
  public long incrby(Region<RedisKey, RedisData> region, RedisKey key,
      long increment) throws NumberFormatException, ArithmeticException {
    byte[] newValue = Coder.longToBytes(increment);
    region.put(key, new RedisString(new ByteArrayWrapper(newValue)));
    return increment;
  }

  @Override
  public BigDecimal incrbyfloat(Region<RedisKey, RedisData> region, RedisKey key,
      BigDecimal increment) throws NumberFormatException, ArithmeticException {
    byte[] newValue = Coder.bigDecimalToBytes(increment);
    region.put(key, new RedisString(new ByteArrayWrapper(newValue)));
    return increment;
  }

  @Override
  public long decr(Region<RedisKey, RedisData> region, RedisKey key)
      throws NumberFormatException, ArithmeticException {
    region.put(key, new RedisString(new ByteArrayWrapper(Coder.stringToBytes("-1"))));
    return -1;
  }

  @Override
  public long decrby(Region<RedisKey, RedisData> region, RedisKey key, long decrement) {
    byte[] newValue = Coder.longToBytes(-decrement);
    region.put(key, new RedisString(new ByteArrayWrapper(newValue)));
    return -decrement;
  }

  @Override
  public int append(ByteArrayWrapper appendValue, Region<RedisKey, RedisData> region,
      RedisKey key) {
    region.put(key, new RedisString(appendValue));
    return appendValue.length();
  }

  @Override
  public ByteArrayWrapper getset(Region<RedisKey, RedisData> region, RedisKey key,
      ByteArrayWrapper value) {
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
      region.put(key, new RedisString(new ByteArrayWrapper(newBytes)));
    }
    return newBytes.length;
  }

  /**
   * SET is currently mostly implemented here. It does not have an implementation on
   * RedisString which is a bit odd.
   */
  public boolean set(CommandHelper helper, RedisKey key, ByteArrayWrapper value,
      SetOptions options) {
    if (options != null) {
      if (options.isNX()) {
        return setnx(helper, key, value, options);
      }

      if (options.isXX() && helper.getRedisData(key).isNull()) {
        return false;
      }
    }

    RedisString redisString = helper.setRedisString(key, value);
    redisString.handleSetExpiration(options);
    return true;
  }

  private boolean setnx(CommandHelper helper, RedisKey key, ByteArrayWrapper value,
      SetOptions options) {
    if (helper.getRedisData(key).exists()) {
      return false;
    }
    RedisString redisString = new RedisString(value);
    redisString.handleSetExpiration(options);
    helper.getRegion().put(key, redisString);
    return true;
  }

  /**
   * bitop is currently only implemented here. It does not have an implementation on
   * RedisString which is a bit odd. This implementation only has a couple of places
   * that care if a RedisString for "key" exists.
   */
  public int bitop(CommandHelper helper, String operation, RedisKey key, List<RedisKey> sources) {
    List<ByteArrayWrapper> sourceValues = new ArrayList<>();
    int selfIndex = -1;
    // Read all the source values, except for self, before locking the stripe.
    RedisStringCommands commander =
        new RedisStringCommandsFunctionInvoker(helper.getRegion());
    for (RedisKey sourceKey : sources) {
      if (sourceKey.equals(key)) {
        // get self later after the stripe is locked
        selfIndex = sourceValues.size();
        sourceValues.add(null);
      } else {
        sourceValues.add(commander.get(sourceKey));
      }
    }
    int indexOfSelf = selfIndex;
    StripedExecutor stripedExecutor = helper.getStripedExecutor();
    return stripedExecutor.execute(key,
        () -> doBitOp(helper, operation, key, indexOfSelf, sourceValues));
  }

  private enum BitOp {
    AND, OR, XOR
  }

  private int doBitOp(CommandHelper helper, String operation, RedisKey key, int selfIndex,
      List<ByteArrayWrapper> sourceValues) {
    if (selfIndex != -1) {
      RedisString redisString = helper.getRedisString(key, true);
      if (!redisString.isNull()) {
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
    if (newValue.length() == 0) {
      helper.getRegion().remove(key);
    } else {
      helper.setRedisString(key, newValue);
    }
    return newValue.length();
  }

  private ByteArrayWrapper doBitOp(BitOp bitOp, List<ByteArrayWrapper> sourceValues, int max) {
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

}
