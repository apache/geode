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

package org.apache.geode.benchmark.jmh.profilers;

import static org.openjdk.jmh.results.AggregationPolicy.AVG;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

public class BufferPoolProfiler implements InternalProfiler {

  private final List<BufferPoolMXBean> pools;
  private final long[] previousCount;
  private final long[] previousMemoryUsed;
  private final long[] previousTotalCapacity;

  public BufferPoolProfiler() {
    pools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
    previousCount = new long[pools.size()];
    previousMemoryUsed = new long[pools.size()];
    previousTotalCapacity = new long[pools.size()];
  }

  @Override
  public String getDescription() {
    return "BufferPool Profiler";
  }

  @Override
  public void beforeIteration(BenchmarkParams benchmarkParams, IterationParams iterationParams) {
    int i = 0;
    for (final BufferPoolMXBean pool : pools) {
      System.out.printf("BufferPool.beforeIteration: %s, %d, %d, %d\n", pool.getName(), pool.getCount(), pool.getMemoryUsed(), pool.getTotalCapacity());
      previousCount[i] = pool.getCount();
      previousMemoryUsed[i] =  pool.getMemoryUsed();
      previousTotalCapacity[i] = pool.getTotalCapacity();
      ++i;
    }
  }

  @Override
  public Collection<? extends Result> afterIteration(BenchmarkParams benchmarkParams,
                                                     IterationParams iterationParams,
                                                     IterationResult result) {
    final ArrayList<ScalarResult> results = new ArrayList<>(pools.size());

    int i = 0;
    for (BufferPoolMXBean pool : pools) {
//      System.out.printf("BufferPool.afterIteration: %s, %d, %d, %d\n", pool.getName(), pool.getCount(), pool.getMemoryUsed(), pool.getTotalCapacity());
      if (pool.getCount() == 0) {
        continue;
      }
      results.add(new ScalarResult(pool.getName() + ".count", pool.getCount() - previousCount[i], "B", AVG));
      results.add(new ScalarResult(pool.getName() + ".memoryUsed", pool.getMemoryUsed() - previousMemoryUsed[i], "B", AVG));
      results.add(new ScalarResult(pool.getName() + ".totalCapacity", pool.getTotalCapacity() - previousTotalCapacity[i], "B", AVG));
      ++i;
    }

    return results;
  }
}