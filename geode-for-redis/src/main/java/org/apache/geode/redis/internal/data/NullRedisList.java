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

import java.util.Collections;
import java.util.List;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.commands.RedisCommandType;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

class NullRedisList extends RedisList {

  NullRedisList() {
    super();
  }

  @Override
  public List<byte[]> lrange(int start, int stop) {
    return Collections.emptyList();
  }

  @Override
  public boolean isNull() {
    return true;
  }

  @Override
  public int linsert(byte[] elementToInsert, byte[] referenceElement, boolean before,
      Region<RedisKey, RedisData> region, RedisKey key) {
    return 0;
  }

  @Override
  public int lrem(int count, byte[] element, Region<RedisKey, RedisData> region, RedisKey key) {
    return 0;
  }

  @Override
  public long lpush(ExecutionHandlerContext context, List<byte[]> elementsToAdd, RedisKey key,
      boolean onlyIfExists) {
    if (onlyIfExists) {
      return 0;
    }

    RedisList newList = new RedisList();
    for (byte[] element : elementsToAdd) {
      newList.elementPushHead(element);
    }
    context.getRegion().create(key, newList);
    context.fireEvent(RedisCommandType.LPUSH, key);

    return elementsToAdd.size();
  }

  @Override
  public long rpush(ExecutionHandlerContext context, List<byte[]> elementsToAdd, RedisKey key,
      final boolean onlyIfExists) {
    if (onlyIfExists) {
      return 0;
    }

    RedisList newList = new RedisList();
    for (byte[] element : elementsToAdd) {
      newList.elementPushTail(element);
    }
    context.getRegion().create(key, newList);
    context.fireEvent(RedisCommandType.RPUSH, key);

    return elementsToAdd.size();
  }

  @Override
  public byte[] lpop(Region<RedisKey, RedisData> region, RedisKey key) {
    return null;
  }

  @Override
  public int llen() {
    return 0;
  }

  @Override
  public byte[] rpop(Region<RedisKey, RedisData> region, RedisKey key) {
    return null;
  }
}
