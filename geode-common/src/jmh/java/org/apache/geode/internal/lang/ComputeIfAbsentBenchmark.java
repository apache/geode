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

package org.apache.geode.internal.lang;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;


/**
 * Test spins up threads that constantly do computeIfAbsent
 * The tests will measure throughput
 * The benchmark tests computeIfAbsent in the presence of other threads contending for the same key
 *
 * Java 8:
 *
 * <pre>
 * Benchmark                                (impl)   Mode  Cnt           Score           Error  Units
 * ComputeIfAbsentBenchmark.group             noop  thrpt    5  1681567966.242 ± 100838863.427  ops/s
 * ComputeIfAbsentBenchmark.group           direct  thrpt    5    10085547.968 ±    242865.843  ops/s
 * ComputeIfAbsentBenchmark.group       workaround  thrpt    5   901693027.158 ±  24905576.450  ops/s
 * </pre>
 *
 * Java 11:
 *
 * <pre>
 * Benchmark                                (impl)   Mode  Cnt           Score           Error  Units
 * ComputeIfAbsentBenchmark.group             noop  thrpt    5  1550792591.631 ± 112841732.003  ops/s
 * ComputeIfAbsentBenchmark.group           direct  thrpt    5   545381612.922 ±  82857066.730  ops/s
 * ComputeIfAbsentBenchmark.group       workaround  thrpt    5   954400194.346 ±  63544640.622  ops/s
 * </pre>
 */
@State(Scope.Benchmark)
@Fork(1)
public class ComputeIfAbsentBenchmark {

  public Map<Integer, Integer> map = new ConcurrentHashMap<>();

  public enum Impl {
    noop, direct, workaround
  }

  @Param()
  public Impl impl;
  public Function<Integer, Integer> accessor;

  final Function<Integer, Integer> noopAccessor = t -> t;
  final Function<Integer, Integer> directAccessor = t -> map.computeIfAbsent(t, k -> k);
  final Function<Integer, Integer> workaroundAccessor =
      t -> JavaWorkarounds.computeIfAbsent(map, t, k -> k);

  @Setup
  public void setup() {
    switch (impl) {
      case noop:
        accessor = noopAccessor;
        break;
      case direct:
        accessor = directAccessor;
        break;
      case workaround:
        accessor = workaroundAccessor;
        break;
      default:
        throw new IllegalStateException();
    }
  }

  @Benchmark
  @Group()
  @GroupThreads(10)
  public Integer load() {
    return accessor.apply(1);
  }

  @Benchmark
  @Group()
  @GroupThreads(1)
  public Integer work() {
    return accessor.apply(1);
  }

}
