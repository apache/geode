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

package org.apache.geode.cache.query.partitioned;


import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.EntryNotFoundException;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class PRClearIntegrationTest {

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withAutoStart();

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Test
  public void doesNotHangWhenClearWithConcurrentPutsAndInvalidates() throws Exception {
    InternalCache cache = server.getCache();
    Region<Object, Object> region = server.createPartitionRegion("regionA", f -> {
    }, f -> f.setTotalNumBuckets(1));
    cache.getQueryService().createIndex("indexA", "r", "/regionA r");
    region.put(0, "value0");

    CompletableFuture<Void> put = executor.runAsync(() -> {
      Thread.currentThread().setName("put-Thread");
      IntStream.range(0, 10).forEach(i -> region.put(i, "value" + i));
    });

    CompletableFuture<Void> invalidate = executor.runAsync(() -> {
      Thread.currentThread().setName("invalidate-Thread");
      IntStream.range(0, 10).forEach(i -> {
        try {
          region.invalidate(i);
        } catch (EntryNotFoundException e) {
          // ignore
        }
      });
    });

    CompletableFuture<Void> clear = executor.runAsync(() -> {
      Thread.currentThread().setName("Clear-Thread");
      IntStream.range(0, 10).forEach(i -> region.clear());
    });

    put.get(5, TimeUnit.SECONDS);
    invalidate.get(5, TimeUnit.SECONDS);
    clear.get(5, TimeUnit.SECONDS);
  }
}
