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

package org.apache.geode.redis.internal.executor;

import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;

import java.util.Collections;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.execute.Execution;
import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.FunctionService;
import org.apache.geode.internal.cache.PrimaryBucketLockException;
import org.apache.geode.redis.internal.data.RedisData;
import org.apache.geode.redis.internal.data.RedisKey;

public abstract class RedisCommandsFunctionInvoker {
  protected final Region<RedisKey, RedisData> region;

  protected RedisCommandsFunctionInvoker(Region<RedisKey, RedisData> region) {
    this.region = region;
  }

  protected <T> T invoke(String functionId, Object filter, Object... arguments) {
    do {
      SingleResultCollector<T> resultsCollector = new SingleResultCollector<>();
      try {
        Execution<Object[], T, T> execution = uncheckedCast(FunctionService.onRegion(region));
        execution
            .withFilter(Collections.singleton(filter))
            .setArguments(arguments)
            .withCollector(resultsCollector)
            .execute(functionId)
            .getResult();
        return resultsCollector.getResult();
      } catch (PrimaryBucketLockException ex) {
        // try again
      } catch (FunctionException ex) {
        if (ex.getMessage()
            .equals("Function named " + functionId + " is not registered to FunctionService")) {
          // try again. A race exists because the data region is created first
          // and then the function is registered.
          continue;
        }
        Throwable initialCause = CommandFunction.getInitialCause(ex);
        if (initialCause instanceof PrimaryBucketLockException) {
          // try again
          continue;
        }
        throw ex;
      }
    } while (true);
  }

  protected <T> T invokeCommandFunction(RedisKey key, Object... arguments) {
    return invoke(CommandFunction.ID, key, arguments);
  }
}
