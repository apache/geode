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
package org.apache.geode.redis.internal.executor.string;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;

public class RedisStringInRegion implements RedisStringCommands {
  private final Region<ByteArrayWrapper, RedisString> region;

  @SuppressWarnings("unchecked")
  public RedisStringInRegion(Region<ByteArrayWrapper, RedisString> region) {
    this.region = region;
  }

  @Override
  public long append(ByteArrayWrapper key, ByteArrayWrapper valueToAppend) {
    RedisString redisString = region.get(key);

    if (redisString != null) {
      return redisString.append(valueToAppend, region, key);
    } else {
      region.create(key, new RedisString(valueToAppend));
      return valueToAppend.length();
    }
  }

  @Override
  public ByteArrayWrapper get(ByteArrayWrapper key) {
    RedisString redisString = region.get(key);

    if (redisString != null) {
      return redisString.get(region, key);
    } else {
      return null;
    }
  }

  @Override
  public RedisString set(ByteArrayWrapper key, ByteArrayWrapper value, SetOptions options) {
    RedisString redisString = region.get(key);

    if (options.getExists().equals(SetOptions.Exists.NX) && redisString != null) {
      return null;
    }

    if (options.getExists().equals(SetOptions.Exists.XX) && redisString == null) {
      return null;
    }

    if (redisString == null) {
      RedisString newValue = new RedisString(value);
      region.create(key, newValue);
      return newValue;
    }
    return redisString.set(value, region, key);
  }

  public Boolean setnx(ByteArrayWrapper key, ByteArrayWrapper value) {
    return new RedisString().setnx(key, region, value);
  }
}
