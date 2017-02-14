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
package org.apache.geode.internal.cache.execute.util;

import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

public class SynchronizedResultCollector<T, S> implements ResultCollector<T, S> {

  public final ResultCollector<T, S> collector;

  public SynchronizedResultCollector(final ResultCollector collector) {
    this.collector = collector;
  }

  @Override
  public synchronized S getResult() throws FunctionException {
    return collector.getResult();
  }

  @Override
  public synchronized S getResult(final long timeout, final TimeUnit unit)
      throws FunctionException, InterruptedException {
    return collector.getResult(timeout, unit);
  }

  @Override
  public synchronized void addResult(final DistributedMember memberID,
      final T resultOfSingleExecution) {
    collector.addResult(memberID, resultOfSingleExecution);
  }

  @Override
  public synchronized void endResults() {
    collector.endResults();
  }

  @Override
  public synchronized void clearResults() {
    collector.clearResults();
  }
}
