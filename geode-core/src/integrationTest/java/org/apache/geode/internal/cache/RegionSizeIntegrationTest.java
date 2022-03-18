/*
 *
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

package org.apache.geode.internal.cache;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.rules.ExecutorServiceRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RegionSizeIntegrationTest {

  @Rule
  public ExecutorServiceRule executor = new ExecutorServiceRule();

  @Rule
  public ServerStarterRule server = new ServerStarterRule().withNoCacheServer().withAutoStart();

  // Before the fix the test would fail consistently if we put a sleep statement inside the
  // LocalRegion.getRegionSize() method after we got the region map size.
  @Test
  public void regionSizeShouldAlwaysBePositive() throws ExecutionException, InterruptedException {
    LocalRegion region = (LocalRegion) server.createRegion(RegionShortcut.REPLICATE, "testRegion");
    CompletableFuture<Void> sizeOp = executor.runAsync(() -> {
      IntStream.range(0, 1000).forEach(i -> {
        assertThat(region.getRegionSize()).isGreaterThanOrEqualTo(0);
      });
    });

    CompletableFuture<Void> putAndDestroyOp = executor.runAsync(() -> {
      IntStream.range(0, 1000).forEach(i -> {
        region.put("key" + i, "value" + i);
        region.destroy("key" + i);
      });
    });

    sizeOp.get();
    putAndDestroyOp.get();
  }
}
