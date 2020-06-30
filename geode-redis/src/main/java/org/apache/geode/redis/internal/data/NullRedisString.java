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

import org.apache.geode.cache.Region;
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
  public int bitpos(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key, int bit,
      int start, Integer end) {
    if (bit == 0) {
      return 0;
    } else {
      return -1;
    }
  }

  @Override
  public int setbit(
      Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key, int bitValue, int byteIndex, byte bitIndex) {
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
  public long incr(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key)
      throws NumberFormatException, ArithmeticException {
    byte[] newValue = {Coder.NUMBER_1_BYTE};
    region.put(key, new RedisString(new ByteArrayWrapper(newValue)));
    return 1;
  }

  @Override
  public long incrby(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      long increment) throws NumberFormatException, ArithmeticException {
    byte[] newValue = Coder.longToBytes(increment);
    region.put(key, new RedisString(new ByteArrayWrapper(newValue)));
    return increment;
  }

  @Override
  public double incrbyfloat(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      double increment) throws NumberFormatException, ArithmeticException {
    byte[] newValue = Coder.doubleToBytes(increment);
    region.put(key, new RedisString(new ByteArrayWrapper(newValue)));
    return increment;
  }

  @Override
  public long decr(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key)
      throws NumberFormatException, ArithmeticException {
    region.put(key, new RedisString(new ByteArrayWrapper(Coder.stringToBytes("-1"))));
    return -1;
  }

  @Override
  public long decrby(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      long decrement) {
    byte[] newValue = Coder.longToBytes(-decrement);
    region.put(key, new RedisString(new ByteArrayWrapper(newValue)));
    return -decrement;
  }

  @Override
  public int append(ByteArrayWrapper appendValue,
      Region<ByteArrayWrapper, RedisData> region,
      ByteArrayWrapper key) {
    region.put(key, new RedisString(appendValue));
    return appendValue.length();
  }

  @Override
  public ByteArrayWrapper getset(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key,
      ByteArrayWrapper value) {
    region.put(key, new RedisString(value));
    return null;
  }

  @Override
  public int setrange(Region<ByteArrayWrapper, RedisData> region, ByteArrayWrapper key, int offset,
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
}
