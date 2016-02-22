/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal.redis.executor;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.internal.redis.ByteArrayWrapper;
import com.gemstone.gemfire.internal.redis.ExecutionHandlerContext;
import com.gemstone.gemfire.internal.redis.Executor;
import com.gemstone.gemfire.internal.redis.RedisDataType;
import com.gemstone.gemfire.internal.redis.RedisDataTypeMismatchException;
import com.gemstone.gemfire.internal.redis.RegionProvider;
import com.gemstone.gemfire.redis.GemFireRedisServer;

/**
 * The AbstractExecutor is the base of all {@link Executor} types for the 
 * {@link GemFireRedisServer}. 
 * 
 * @author Vitaliy Gavrilov
 *
 */
public abstract class AbstractExecutor implements Executor {

  /**
   * Number of Regions used by GemFireRedisServer internally
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
   * Getter method for a {@link Region} in the case that a Region should be
   * created if one with the given name does not exist. Before getting or creating
   * a Region, a check is first done to make sure the desired key doesn't already
   * exist with a different {@link RedisDataType}. If there is a data type mismatch
   * this method will throw a {@link RuntimeException}.
   * 
   * ********************** IMPORTANT NOTE **********************************************
   * This method will not fail in returning a Region unless an internal error occurs, so
   * if a Region is destroyed right after it is created, it will attempt to retry until a 
   * reference to that Region is obtained
   * *************************************************************************************
   * 
   * @param context Client client
   * @param key String key of desired key
   * @param type Type of data type desired
   * @return Region with name key
   */
  protected Region<?, ?> getOrCreateRegion(ExecutionHandlerContext context, ByteArrayWrapper key, RedisDataType type) {
    return context.getRegionProvider().getOrCreateRegion(key, type, context);
  }

  /**
   * Checks if the given key is associated with the passed data type.
   * If there is a mismatch, a {@link RuntimeException} is thrown
   * 
   * @param key Key to check
   * @param type Type to check to
   * @param context context
   */
  protected void checkDataType(ByteArrayWrapper key, RedisDataType type, ExecutionHandlerContext context) {
    RedisDataType currentType = context.getRegionProvider().getRedisDataType(key);
    if (currentType == null)
      return;
    if (currentType == RedisDataType.REDIS_PROTECTED)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is protected");
    if (currentType != type)
      throw new RedisDataTypeMismatchException("The key name \"" + key + "\" is already used by a " + currentType.toString());
  }

  protected Query getQuery(ByteArrayWrapper key, Enum<?> type, ExecutionHandlerContext context) {
    return context.getRegionProvider().getQuery(key, type);
  }

  protected boolean removeEntry(ByteArrayWrapper key, RedisDataType type, ExecutionHandlerContext context) {
    if (type == null || type == RedisDataType.REDIS_PROTECTED)
      return false;
    RegionProvider rC = context.getRegionProvider();
    return rC.removeKey(key, type);
  }

  protected int getBoundedStartIndex(int index, int size) {
    if (size <  0)
      throw new IllegalArgumentException("Size < 0, really?");
    if (index >= 0)
      return Math.min(index, size);
    else
      return Math.max(index + size, 0);
  }

  protected int getBoundedEndIndex(int index, int size) {
    if (size <  0)
      throw new IllegalArgumentException("Size < 0, really?");
    if (index >= 0)
      return Math.min(index, size);
    else
      return Math.max(index + size, -1);
  }

  protected long getBoundedStartIndex(long index, long size) {
    if (size <  0L)
      throw new IllegalArgumentException("Size < 0, really?");
    if (index >= 0L)
      return Math.min(index, size);
    else
      return Math.max(index + size, 0);
  }

  protected long getBoundedEndIndex(long index, long size) {
    if (size <  0L)
      throw new IllegalArgumentException("Size < 0, really?");
    if (index >= 0L)
      return Math.min(index, size);
    else
      return Math.max(index + size, -1);
  }
}
