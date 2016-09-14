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
package org.apache.geode.redis.internal.executor.sortedset;

import org.apache.geode.cache.Region;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.DoubleWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisDataType;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public abstract class SortedSetExecutor extends AbstractExecutor {

  @Override
  protected Region<ByteArrayWrapper, DoubleWrapper> getOrCreateRegion(ExecutionHandlerContext context, ByteArrayWrapper key, RedisDataType type) {
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, DoubleWrapper> r = (Region<ByteArrayWrapper, DoubleWrapper>) context.getRegionProvider().getOrCreateRegion(key, type, context);
    return r;
  }
  
  protected Region<ByteArrayWrapper, DoubleWrapper> getRegion(ExecutionHandlerContext context, ByteArrayWrapper key) {
    @SuppressWarnings("unchecked")
    Region<ByteArrayWrapper, DoubleWrapper> r = (Region<ByteArrayWrapper, DoubleWrapper>) context.getRegionProvider().getRegion(key);
    return r;
  }
  
}
