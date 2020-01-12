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
 * This test simulates an existing partitioned region with evenly distributed numbers of buckets,
 * all of which are exactly the same size and with no replicate copies. A single new member is added
 * to the cluster and the region is then rebalanced. This represents a best-case scenario for
 * rebalancing a cluster after the addition of a member.
 */
@State(Scope.Thread)
@Fork(1)
public class RebalanceOnAddingMemberBenchmark {

  @Param({"4", "8", "16", "32", "64", "128"})
  public int startingMembers;

  @Param({"100", "200", "400", "800", "1600", "3200"})
  public int totalBuckets;

  @Benchmark
  @Measurement(time = 5, iterations = 10)
  @Warmup(iterations = 5)
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public int addNewMemberToBalancedRegion() throws UnknownHostException {
    RebalanceModelBuilder modelBuilder = new RebalanceModelBuilder(startingMembers, totalBuckets);
    PartitionedRegionLoadModel model = modelBuilder.withNewMembers(1).createModel();
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
