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


import java.util.concurrent.Callable;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.RegionProvider;

/**
 * Provides some of the common code shared by all FunctionExecutor classes.
 * The actual executors are subclasses of this class.
 * These FunctionExecutor classes are used on the server that is actually
 * running a function that was invoked by a *FunctionInvoker class.
 */
public abstract class RedisDataCommandsFunctionExecutor {
  private final RegionProvider regionProvider;

  protected RegionProvider getRegionProvider() {
    return regionProvider;
  }

  protected RedisDataCommandsFunctionExecutor(RegionProvider regionProvider) {
    this.regionProvider = regionProvider;
  }

  protected Region<RedisKey, RedisData> getRegion() {
    return regionProvider.getDataRegion();
  }

  protected <T> T stripedExecute(Object key, Callable<T> callable) {
    return regionProvider.execute(key, callable);
  }

  protected RedisData getRedisData(RedisKey key) {
    return regionProvider.getRedisData(key);
  }
}
