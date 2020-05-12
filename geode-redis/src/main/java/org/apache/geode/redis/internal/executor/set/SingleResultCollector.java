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

import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

public class SingleResultCollector<T> implements ResultCollector<T, T> {
  T result;

  @Override
  public T getResult() throws FunctionException {
    return result;
  }

  @Override
  public T getResult(long timeout, TimeUnit unit)
      throws FunctionException {
    return result;
  }

  @Override
  public void addResult(DistributedMember memberID, T resultOfSingleExecution) {
    result = resultOfSingleExecution;
  }

  @Override
  public void endResults() {}

  @Override
  public void clearResults() {
    result = null;
  }
}
