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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.FunctionContext;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.AllowExecutionInLowMemory;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;

public abstract class SingleResultRedisFunction implements AllowExecutionInLowMemory {

  private static final long serialVersionUID = 3239452234149879302L;
  private final transient PartitionedRegion partitionedRegion;

  public SingleResultRedisFunction(Region<RedisKey, RedisData> dataRegion) {
    this.partitionedRegion = (PartitionedRegion) dataRegion;
  }

  protected abstract Object compute(RedisKey key, Object[] args);

  @Override
  public void execute(FunctionContext<Object[]> context) {

    RegionFunctionContextImpl regionFunctionContext = (RegionFunctionContextImpl) context;

    RedisKey key = (RedisKey) regionFunctionContext.getFilter().iterator().next();

    Object[] args = context.getArguments();

    Runnable computation = () -> {
      Object result = compute(key, args);
      context.getResultSender().lastResult(result);
    };

    partitionedRegion.computeWithPrimaryLocked(key, computation);
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public boolean isHA() {
    return false;
  }
}
