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

package org.apache.geode.internal.util.concurrent;

import static org.openjdk.jmh.infra.Blackhole.consumeCPU;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.jctools.maps.NonBlockingHashMapLong;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import org.apache.geode.benchmark.jmh.profilers.ObjectSizeProfiler;

/**
 * Benchmark multi-producer multi-consumer maps with {@code long} keys.
 *
 * <pre>
 * Benchmark                                                                       (impl)   Mode  Cnt          Score   Error   Units
 * MpmcMapLongBenchmark.workload                                                     Noop  thrpt       368824491.236           ops/s
 * MpmcMapLongBenchmark.workload:objectSize.map                                      Noop  thrpt              16.000           bytes
 * MpmcMapLongBenchmark.workload:·gc.alloc.rate                                      Noop  thrpt               0.592          MB/sec
 * MpmcMapLongBenchmark.workload:·gc.alloc.rate.norm                                 Noop  thrpt               0.002            B/op
 * MpmcMapLongBenchmark.workload:·gc.count                                           Noop  thrpt                 ≈ 0          counts
 * MpmcMapLongBenchmark.workload                                        ConcurrentHashMap  thrpt        40144803.580           ops/s
 * MpmcMapLongBenchmark.workload:objectSize.map                         ConcurrentHashMap  thrpt        74100952.000           bytes
 * MpmcMapLongBenchmark.workload:·gc.alloc.rate                         ConcurrentHashMap  thrpt            1748.973          MB/sec
 * MpmcMapLongBenchmark.workload:·gc.alloc.rate.norm                    ConcurrentHashMap  thrpt              47.994            B/op
 * MpmcMapLongBenchmark.workload:·gc.churn.PS_Eden_Space                ConcurrentHashMap  thrpt            1881.136          MB/sec
 * MpmcMapLongBenchmark.workload:·gc.churn.PS_Eden_Space.norm           ConcurrentHashMap  thrpt              51.621            B/op
 * MpmcMapLongBenchmark.workload:·gc.churn.PS_Survivor_Space            ConcurrentHashMap  thrpt               0.027          MB/sec
 * MpmcMapLongBenchmark.workload:·gc.churn.PS_Survivor_Space.norm       ConcurrentHashMap  thrpt               0.001            B/op
 * MpmcMapLongBenchmark.workload:·gc.count                              ConcurrentHashMap  thrpt               8.000          counts
 * MpmcMapLongBenchmark.workload:·gc.time                               ConcurrentHashMap  thrpt              55.000              ms
 * MpmcMapLongBenchmark.workload                                   NonBlockingHashMapLong  thrpt        63476101.700           ops/s
 * MpmcMapLongBenchmark.workload:objectSize.map                    NonBlockingHashMapLong  thrpt        28591576.000           bytes
 * MpmcMapLongBenchmark.workload:·gc.alloc.rate                    NonBlockingHashMapLong  thrpt               0.010          MB/sec
 * MpmcMapLongBenchmark.workload:·gc.alloc.rate.norm               NonBlockingHashMapLong  thrpt              ≈ 10⁻⁴            B/op
 * MpmcMapLongBenchmark.workload:·gc.count                         NonBlockingHashMapLong  thrpt                 ≈ 0          counts
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@State(Scope.Benchmark)
public class MpmcMapLongBenchmark {

  public enum Impl {
    Noop, ConcurrentHashMap, NonBlockingHashMapLong
  }

  @Param
  public Impl impl;
  private Accessor accessor;

  @Setup
  public void setup() {
    switch (impl) {
      case Noop:
        accessor = new Accessor() {
          @Override
          public Object get(final long key) {
            return null;
          }

          @Override
          public Object put(final long key, final Object value) {
            return null;
          }

          @Override
          public Object createValue(final long key) {
            return null;
          }

          @Override
          public Map<Long, Object> getMap() {
            return Collections.emptyMap();
          }
        };
        break;
      case ConcurrentHashMap:
        accessor = new Accessor() {
          final ConcurrentHashMap<Long, Object> map = new ConcurrentHashMap<>();

          @Override
          public Object get(final long key) {
            return map.get(key);
          }

          @Override
          public Object put(final long key, final Object value) {
            return map.put(key, value);
          }

          @Override
          public ConcurrentHashMap<Long, Object> getMap() {
            return map;
          }
        };
        break;
      case NonBlockingHashMapLong:
        accessor = new Accessor() {
          final NonBlockingHashMapLong<Object> map = new NonBlockingHashMapLong<>();

          @Override
          public Object get(final long key) {
            return map.get(key);
          }

          @Override
          public Object put(final long key, final Object value) {
            return map.put(key, value);
          }

          @Override
          public NonBlockingHashMapLong<Object> getMap() {
            return map;
          }
        };
        break;
    }

    ObjectSizeProfiler.objectSize("map", accessor.getMap());
  }

  @TearDown
  public void tearDown() {}

  @Benchmark
  public void workload() {
    long key = randomKey();
    Object value = accessor.get(key);
    if (null == value) {
      value = accessor.createValue(key);
    }
    consumeCPU(10);
    accessor.put(key, value);
  }

  private long randomKey() {
    return ThreadLocalRandom.current().nextInt(0, 1_000_000);
  }

  private interface Accessor {
    Object get(long key);

    Object put(long key, Object value);

    default Object createValue(long key) {
      return new Object();
    }

    Map<Long, Object> getMap();
  }
}
