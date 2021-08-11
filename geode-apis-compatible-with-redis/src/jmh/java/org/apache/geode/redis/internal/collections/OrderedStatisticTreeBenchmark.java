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
package org.apache.geode.redis.internal.collections;

import java.util.Random;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

public class OrderedStatisticTreeBenchmark {

  private static final int DATA_SIZE = 100_000;

  public enum SetType {
    TREESET,
    ORDER_STATISTICS_TREE
  }

  @State(Scope.Benchmark)
  public static class BenchmarkState {
    @Param({"TREESET", "ORDER_STATISTICS_TREE"})
    private SetType setType;

    private final long[] values = new long[DATA_SIZE];
    private int current = 0;
    private OrderStatisticsSet<Long> set;

    @Setup
    public void generateData() {
      Random random = new Random(0);
      createSet();
      for (int i = 0; i < DATA_SIZE; i++) {
        values[i] = random.nextLong();
        set.add(values[i]);
      }
    }

    private void createSet() {
      switch (setType) {
        case TREESET:
          set = new IndexibleTreeSet<>();
          break;
        case ORDER_STATISTICS_TREE:
          set = new OrderStatisticsTree<>();
      }
    }
  }

  @Benchmark
  public boolean contains(BenchmarkState state) {
    state.current++;
    state.current = state.current % DATA_SIZE;
    return state.set.contains(state.values[state.current]);
  }

  @Benchmark
  public int indexOf(BenchmarkState state) {
    state.current++;
    state.current = state.current % DATA_SIZE;
    return state.set.indexOf(state.values[state.current]);
  }

  @Benchmark
  public boolean insert(BenchmarkState state) {
    state.current = state.current % DATA_SIZE;
    if (state.current == 0) {
      state.createSet();
    }
    final boolean result = state.set.add(state.values[state.current]);
    state.current++;
    return result;
  }
}
