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
package org.apache.geode.cache.query.internal;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class JmhConcurrencyLoadGenerator {
  final ScheduledThreadPoolExecutor loadGenerationExecutorService;
  private final int numThreads;

  public JmhConcurrencyLoadGenerator(int numThreads) {

    this.numThreads = numThreads;
    loadGenerationExecutorService =
        (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(
            numThreads);
    System.out.println(String.format("Pool has %d threads", numThreads));
    loadGenerationExecutorService.setRemoveOnCancelPolicy(true);

  }

  public void generateLoad(int delay, TimeUnit timeUnit, Runnable runnable) {
    for (int i = 0; i < numThreads; i++) {
      loadGenerationExecutorService.schedule(runnable, delay, timeUnit);
    }
  }

  public void tearDown() {
    loadGenerationExecutorService.shutdownNow();
  }
}
