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
package org.apache.geode.cache.put;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;

@Fork(1)
public class HeavyContention {

  @State(Scope.Benchmark)
  public static class CacheState {
    private Region replicateRegion;
    private Region localRegion;
    private Region partitionedRegion;

    @Setup
    public void setup() {
      Cache cache = new CacheFactory().set("mcast-port", "0").set("locators", "").create();

      replicateRegion = cache.createRegionFactory(RegionShortcut.REPLICATE).create("ReplicateRegion");
      localRegion = cache.createRegionFactory(RegionShortcut.LOCAL).create("LocalRegion");
      partitionedRegion = cache.createRegionFactory(RegionShortcut.PARTITION).create("PartitionedRegion");
    }
  }

  @Benchmark
  @Threads(100)
  @Warmup(iterations = 1)
  @Measurement(iterations = 5)
  public Object replicate(CacheState state) {
    return state.replicateRegion.put("key", "value");
  }

  @Benchmark
  @Threads(100)
  @Warmup(iterations = 1)
  @Measurement(iterations = 5)
  public Object local(CacheState state) {
    return state.localRegion.put("key", "value");
  }

  @Benchmark
  @Threads(100)
  @Warmup(iterations = 1)
  @Measurement(iterations = 5)
  public Object partition(CacheState state) {
    return state.partitionedRegion.put("key", "value");
  }

  @Benchmark
  @Threads(100)
  @Warmup(iterations = 1)
  @Measurement(iterations = 5)
  public void noop() {
  }
}
