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
package org.apache.geode.cache;

import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.geode.internal.lang.SystemPropertyHelper;

@State(Scope.Benchmark)
@Fork(1)
public class EvictionMultiThreadedPerformanceBenchmark {
  private static final int MAX_ENTRIES = 1_000_000;

  @Param({"true", "false"})
  public String useAsync;

  Cache cache;
  Region<String, String> region;
  AtomicInteger nextKey = new AtomicInteger(MAX_ENTRIES + 1);

  @Setup(Level.Trial)
  public void setup() {
    System.setProperty("geode." + SystemPropertyHelper.EVICTION_SCAN_ASYNC, useAsync);
    cache = new CacheFactory().set(LOG_LEVEL, "warn").create();
    region = createRegion(cache, MAX_ENTRIES);
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    cache.close();
  }

  @Benchmark
  @Measurement(time = 5, iterations = 10)
  @Warmup(iterations = 5)
  @BenchmarkMode(Mode.Throughput)
  @Threads(2)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public String evictingCreate1Thread() {
    String key = Integer.toString(nextKey.incrementAndGet());
    return region.put(key, "value");
  }

  @Benchmark
  @Measurement(time = 5, iterations = 10)
  @Warmup(iterations = 5)
  @BenchmarkMode(Mode.Throughput)
  @Threads(2)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public String evictingCreate2Threads() {
    String key = Integer.toString(nextKey.incrementAndGet());
    return region.put(key, "value");
  }

  @Benchmark
  @Measurement(time = 5, iterations = 10)
  @Warmup(iterations = 5)
  @BenchmarkMode(Mode.Throughput)
  @Threads(4)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public String evictingCreate4Threads() {
    String key = Integer.toString(nextKey.incrementAndGet());
    return region.put(key, "value");
  }

  @Benchmark
  @Measurement(time = 5, iterations = 10)
  @Warmup(iterations = 5)
  @BenchmarkMode(Mode.Throughput)
  @Threads(8)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public String evictingCreate8Threads() {
    String key = Integer.toString(nextKey.incrementAndGet());
    return region.put(key, "value");
  }

  @Benchmark
  @Measurement(time = 5, iterations = 10)
  @Warmup(iterations = 5)
  @BenchmarkMode(Mode.Throughput)
  @Threads(16)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public String evictingCreate16Threads() {
    String key = Integer.toString(nextKey.incrementAndGet());
    return region.put(key, "value");
  }

  private Region<String, String> createRegion(Cache cache, int maxSize) {
    Region<String, String> region = cache.<String, String>createRegionFactory(RegionShortcut.LOCAL)
        .setEvictionAttributes(
            EvictionAttributes.createLRUEntryAttributes(maxSize, EvictionAction.LOCAL_DESTROY))
        .create("testRegion");
    for (int i = 0; i < MAX_ENTRIES; i++) {
      region.put(Integer.toString(i), "value");
    }
    region.put("over", "limit");
    return region;
  }
}
