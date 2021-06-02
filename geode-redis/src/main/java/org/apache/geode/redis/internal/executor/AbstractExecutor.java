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
package org.apache.geode.redis.internal.executor;

import java.util.Collection;

import io.netty.buffer.ByteBuf;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Query;
import org.apache.geode.redis.GeodeRedisServer;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.Coder;
import org.apache.geode.redis.internal.CoderException;
import org.apache.geode.redis.internal.Command;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.Executor;
import org.apache.geode.redis.internal.RedisConstants;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.RegionProvider;

/**
 * The AbstractExecutor is the base of all {@link Executor} types for the {@link GeodeRedisServer}.
 *
 *
 */
public abstract class AbstractExecutor implements Executor {

  /**
   * Number of Regions used by GeodeRedisServer internally
   */
  public static final int NUM_DEFAULT_REGIONS = 3;

  /**
   * Max length of a list
   */
  protected static final Integer INFINITY_LIMIT = Integer.MAX_VALUE;

  /**
   * Constant of number of milliseconds in a second
   */
  protected static final int millisInSecond = 1000;

  /**
   * Getter method for a {@link Region} in the case that a Region should be created if one with the
   * given name does not exist. Before getting or creating a Region, a check is first done to make
   * sure the desired key doesn't already exist with a different {@link RedisDataType}. If there is
   * a data type mismatch this method will throw a {@link RuntimeException}.
   *
   * ********************** IMPORTANT NOTE ********************************************** This
   * method will not fail in returning a Region unless an internal error occurs, so if a Region is
   * destroyed right after it is created, it will attempt to retry until a reference to that Region
   * is obtained
   * *************************************************************************************
   *
   * @param context Client client
   * @param key String key of desired key
   * @param type Type of data type desired
   * @return Region with name key
   */
  protected Region<?, ?> getOrCreateRegion(ExecutionHandlerContext context, ByteArrayWrapper key,
      RedisDataType type) {
    return context.getRegionProvider().getOrCreateRegion(key, type, context);
  }

  /**
   * Checks if the given key is associated with the passed expectedDataType. If there is a mismatch,
   * a
   * {@link RuntimeException} is thrown
   *
   * @param key Key to check
   * @param expectedDataType Type to check to
   * @param context context
   */
  protected void checkDataType(ByteArrayWrapper key, RedisDataType expectedDataType,
      ExecutionHandlerContext context) {
    context.getKeyRegistrar().validate(key, expectedDataType);
  }

  protected Query getQuery(ByteArrayWrapper key, Enum<?> type, ExecutionHandlerContext context) {
    return context.getRegionProvider().getQuery(key, type);
  }

  protected boolean removeEntry(ByteArrayWrapper key, RedisDataType type,
      ExecutionHandlerContext context) {
    if (type == null || type == RedisDataType.REDIS_PROTECTED) {
      return false;
    }
    RegionProvider rC = context.getRegionProvider();
    return rC.removeKey(key, type);
  }

  protected int getBoundedStartIndex(int index, int size) {
    if (size < 0) {
      throw new IllegalArgumentException("Size < 0, really?");
    }
    if (index >= 0) {
      return Math.min(index, size);
    } else {
      return Math.max(index + size, 0);
    }
  }

  protected int getBoundedEndIndex(int index, int size) {
    if (size < 0) {
      throw new IllegalArgumentException("Size < 0, really?");
    }
    if (index >= 0) {
      return Math.min(index, size);
    } else {
      return Math.max(index + size, -1);
    }
  }

  protected long getBoundedStartIndex(long index, long size) {
    if (size < 0L) {
      throw new IllegalArgumentException("Size < 0, really?");
    }
    if (index >= 0L) {
      return Math.min(index, size);
    } else {
      return Math.max(index + size, 0);
    }
  }

  protected long getBoundedEndIndex(long index, long size) {
    if (size < 0L) {
      throw new IllegalArgumentException("Size < 0, really?");
    }
    if (index >= 0L) {
      return Math.min(index, size);
    } else {
      return Math.max(index + size, -1);
    }
  }

  protected void respondBulkStrings(Command command, ExecutionHandlerContext context,
      Object message) {
    ByteBuf rsp;
    try {
      if (message instanceof Collection) {
        rsp = Coder.getBulkStringArrayResponse(context.getByteBufAllocator(),
            (Collection<?>) message);
      } else {
        rsp = Coder.getBulkStringResponse(context.getByteBufAllocator(), message);
      }
    } catch (CoderException e) {
      command.setResponse(Coder.getErrorResponse(context.getByteBufAllocator(),
          RedisConstants.SERVER_ERROR_MESSAGE));
      return;
    }

    command.setResponse(rsp);
  }
}
