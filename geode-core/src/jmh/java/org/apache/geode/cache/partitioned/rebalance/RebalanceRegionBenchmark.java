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
package org.apache.geode.cache.partitioned.rebalance;

import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.geode.internal.cache.partitioned.rebalance.MoveBuckets;
import org.apache.geode.internal.cache.partitioned.rebalance.RebalanceDirector;
import org.apache.geode.internal.cache.partitioned.rebalance.model.PartitionedRegionLoadModel;

/**
 * This test simulates a worst case scenario for rebalancing a region. Each bucket is of a
 * different size, randomly determined, but defined by a Gaussian distribution. All buckets
 * on a member are either larger or smaller than the average bucket size, which leads to a
 * maximal number of bucket moves being required.
 */
@State(Scope.Thread)
@Fork(1)
public class RebalanceRegionBenchmark {
  private static final int STARTING_MEMBERS = 32;
  private static final int TOTAL_BUCKETS = 800;

  @Param({"0", "5", "10", "15", "20"})
  public int deviation;

  @Benchmark
  @Measurement(time = 5, iterations = 10)
  @Warmup(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int rebalance() throws UnknownHostException {
    RebalanceModelBuilder modelBuilder = new RebalanceModelBuilder(STARTING_MEMBERS, TOTAL_BUCKETS);
    PartitionedRegionLoadModel model =
        modelBuilder.withBucketSizeStandardDeviation(deviation).createModel();
    return doMoves(new MoveBuckets(), model);
  }

  private int doMoves(RebalanceDirector director, PartitionedRegionLoadModel model) {
    int moveCount = 0;

    model.initialize();
    director.initialize(model);
    while (director.nextStep()) {
      moveCount++;
    }

    return moveCount;
  }
}
