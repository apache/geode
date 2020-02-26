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
package org.apache.geode.redis.internal.executor.set;

import java.util.Set;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.TimeoutException;
import org.apache.geode.redis.internal.AutoCloseableLock;
import org.apache.geode.redis.internal.ByteArrayWrapper;
import org.apache.geode.redis.internal.ExecutionHandlerContext;
import org.apache.geode.redis.internal.RedisLockService;
import org.apache.geode.redis.internal.executor.AbstractExecutor;

public abstract class SetExecutor extends AbstractExecutor {

  /**
   * Obtain the region that holds set data
   *
   * @param context the execution handler
   * @return the set Region
   */
  Region<ByteArrayWrapper, Set<ByteArrayWrapper>> getRegion(ExecutionHandlerContext context) {
    return context.getRegionProvider().getSetRegion();
  }

  protected AutoCloseableLock withRegionLock(ExecutionHandlerContext context, ByteArrayWrapper key)
      throws InterruptedException, TimeoutException {
    RedisLockService lockService = context.getLockService();

    return lockService.lock(key);
  }

}
