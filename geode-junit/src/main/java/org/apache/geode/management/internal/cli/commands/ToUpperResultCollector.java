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
package org.apache.geode.management.internal.cli.commands;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.geode.cache.execute.FunctionException;
import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.DistributedMember;

public class ToUpperResultCollector implements ResultCollector {

  private List<Object> results = new ArrayList<>();

  private CountDownLatch latch = new CountDownLatch(1);

  @Override
  public Object getResult() throws FunctionException {
    try {
      latch.await();
    } catch (InterruptedException e) {
      throw new FunctionException("Interrupted waiting for results", e);
    }
    return results;
  }

  @Override
  public Object getResult(long timeout, TimeUnit unit)
      throws FunctionException, InterruptedException {
    latch.await(timeout, unit);
    return results;
  }

  @Override
  public void addResult(DistributedMember memberID, Object resultOfSingleExecution) {
    results.add(resultOfSingleExecution.toString().toUpperCase());
  }

  @Override
  public void endResults() {
    latch.countDown();
  }

  @Override
  public void clearResults() {
    results.clear();
    latch = new CountDownLatch(1);
  }
}
