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
import org.apache.geode.internal.cache.LocalDataSet;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.execute.InternalFunction;
import org.apache.geode.internal.cache.execute.RegionFunctionContextImpl;
import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.data.ByteArrayWrapper;
import org.apache.geode.redis.internal.data.RedisSet;

@SuppressWarnings("unchecked")
public abstract class SingleResultRedisFunction implements InternalFunction<Object[]> {

  protected abstract Object compute(Region<ByteArrayWrapper, RedisSet> localRegion,
      ByteArrayWrapper key, RedisCommandType command,
      Object[] args);

  @Override
  public void execute(FunctionContext<Object[]> context) {

    RegionFunctionContextImpl regionFunctionContext =
        (RegionFunctionContextImpl) context;

    ByteArrayWrapper key =
        (ByteArrayWrapper) regionFunctionContext.getFilter().iterator().next();

    Region<ByteArrayWrapper, RedisSet> localRegion =
        regionFunctionContext.getLocalDataSet(regionFunctionContext.getDataSet());

    Object[] args = context.getArguments();
    RedisCommandType command = (RedisCommandType) args[0];

    Runnable computation = () -> {
      Object result = compute(localRegion, key, command, args);
      context.getResultSender().lastResult(result);
    };

    computeWithPrimaryLocked(key, (LocalDataSet) localRegion, computation);
  }

  @Override
  public boolean optimizeForWrite() {
    return true;
  }

  @Override
  public boolean isHA() {
    return true;
  }

  public static void computeWithPrimaryLocked(Object key, LocalDataSet localDataSet, Runnable r) {
    PartitionedRegion partitionedRegion = localDataSet.getProxy();

    partitionedRegion.computeWithPrimaryLocked(key, r);
  }

}
