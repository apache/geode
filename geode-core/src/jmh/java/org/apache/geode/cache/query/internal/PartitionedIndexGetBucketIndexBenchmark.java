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


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.internal.index.PartitionedIndex;
import org.apache.geode.distributed.DistributedSystem;

/**
 * Test spins up threads that constantly do getBucketIndex
 * The tests will measure throughput
 * The benchmark tests getBucketIndex in the presence of other threads attempting the same operation
 */

@State(Scope.Thread)
@Fork(1)
public class PartitionedIndexGetBucketIndexBenchmark {

  private PartitionedIndex index;
  /*
   * After load is established, how many measurements shall we take?
   */
  private static final double BENCHMARK_ITERATIONS = 10;


  @Setup(Level.Trial)
  public void trialSetup() throws InterruptedException {
    DistributedSystem mockDS = mock(DistributedSystem.class);
    Cache mockCache = mock(Cache.class);
    Region mockRegion = mock(Region.class);
    when(mockRegion.getCache()).thenReturn(mockCache);
    when(mockCache.getDistributedSystem()).thenReturn(mockDS);
    index = new PartitionedIndex(null, null, "", mockRegion, "", "", "");
    index.addToBucketIndexes(mockRegion, mock(Index.class));
  }

  @Group("getBucketIndexThroughput")
  @GroupThreads(10)
  @Benchmark
  public void getBucketIndexLoad() {
    index.getBucketIndex();
  }

  @Benchmark
  @Group("getBucketIndexThroughput")
  @GroupThreads(1)
  @Measurement(iterations = (int) BENCHMARK_ITERATIONS)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  // @Warmup we don't warm up because our @Setup warms us up
  public Object getBucketIndex() {
    return index.getBucketIndex();
  }

}
