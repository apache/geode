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

import static java.util.Collections.emptyList;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.netty.Coder;

public class NullRedisHash extends RedisHash {
  NullRedisHash() {
    super(emptyList());
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  public int hset(Region<RedisKey, RedisData> region, RedisKey key,
      List<byte[]> fieldsToSet, boolean nx) {
    region.put(key, new RedisHash(fieldsToSet));
    return fieldsToSet.size() / 2;
  }

  @Override
  public byte[] hincrby(Region<RedisKey, RedisData> region, RedisKey key,
      byte[] field, long increment)
      throws NumberFormatException, ArithmeticException {
    byte[] newValue = Coder.longToBytes(increment);
    region.put(key,
        new RedisHash(Arrays.asList(field, newValue)));
    return newValue;
  }

  @Override
  public BigDecimal hincrbyfloat(Region<RedisKey, RedisData> region, RedisKey key,
      byte[] field, BigDecimal increment) throws NumberFormatException {
    region.put(key,
        new RedisHash(
            Arrays.asList(field, Coder.bigDecimalToBytes(increment))));
    return increment;
  }
}
