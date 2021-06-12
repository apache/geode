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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.jctools.maps.NonBlockingHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
 * Benchmark multi-producer multi-consumer maps.
 *
 * <pre>
 * Benchmark                                                               (impl)   Mode  Cnt          Score   Error   Units
 * MpmcMapBenchmark.workload                                                 Noop  thrpt       335731972.821           ops/s
 * MpmcMapBenchmark.workload:objectSize.map                                  Noop  thrpt              16.000           bytes
 * MpmcMapBenchmark.workload:·gc.alloc.rate                                  Noop  thrpt            4872.719          MB/sec
 * MpmcMapBenchmark.workload:·gc.alloc.rate.norm                             Noop  thrpt              15.999            B/op
 * MpmcMapBenchmark.workload:·gc.churn.PS_Eden_Space                         Noop  thrpt            4918.333          MB/sec
 * MpmcMapBenchmark.workload:·gc.churn.PS_Eden_Space.norm                    Noop  thrpt              16.149            B/op
 * MpmcMapBenchmark.workload:·gc.churn.PS_Survivor_Space                     Noop  thrpt               0.241          MB/sec
 * MpmcMapBenchmark.workload:·gc.churn.PS_Survivor_Space.norm                Noop  thrpt               0.001            B/op
 * MpmcMapBenchmark.workload:·gc.count                                       Noop  thrpt             180.000          counts
 * MpmcMapBenchmark.workload:·gc.time                                        Noop  thrpt              95.000              ms
 * MpmcMapBenchmark.workload                                    ConcurrentHashMap  thrpt        45633050.227           ops/s
 * MpmcMapBenchmark.workload:objectSize.map                     ConcurrentHashMap  thrpt        66101792.000           bytes
 * MpmcMapBenchmark.workload:·gc.alloc.rate                     ConcurrentHashMap  thrpt             662.851          MB/sec
 * MpmcMapBenchmark.workload:·gc.alloc.rate.norm                ConcurrentHashMap  thrpt              15.998            B/op
 * MpmcMapBenchmark.workload:·gc.churn.PS_Eden_Space            ConcurrentHashMap  thrpt             691.401          MB/sec
 * MpmcMapBenchmark.workload:·gc.churn.PS_Eden_Space.norm       ConcurrentHashMap  thrpt              16.687            B/op
 * MpmcMapBenchmark.workload:·gc.churn.PS_Survivor_Space        ConcurrentHashMap  thrpt               0.024          MB/sec
 * MpmcMapBenchmark.workload:·gc.churn.PS_Survivor_Space.norm   ConcurrentHashMap  thrpt               0.001            B/op
 * MpmcMapBenchmark.workload:·gc.count                          ConcurrentHashMap  thrpt               3.000          counts
 * MpmcMapBenchmark.workload:·gc.time                           ConcurrentHashMap  thrpt              21.000              ms
 * MpmcMapBenchmark.workload                                   NonBlockingHashMap  thrpt        39676741.026           ops/s
 * MpmcMapBenchmark.workload:objectSize.map                    NonBlockingHashMap  thrpt        57174680.000           bytes
 * MpmcMapBenchmark.workload:·gc.alloc.rate                    NonBlockingHashMap  thrpt             576.385          MB/sec
 * MpmcMapBenchmark.workload:·gc.alloc.rate.norm               NonBlockingHashMap  thrpt              15.998            B/op
 * MpmcMapBenchmark.workload:·gc.churn.PS_Eden_Space           NonBlockingHashMap  thrpt             590.183          MB/sec
 * MpmcMapBenchmark.workload:·gc.churn.PS_Eden_Space.norm      NonBlockingHashMap  thrpt              16.381            B/op
 * MpmcMapBenchmark.workload:·gc.churn.PS_Survivor_Space       NonBlockingHashMap  thrpt               1.288          MB/sec
 * MpmcMapBenchmark.workload:·gc.churn.PS_Survivor_Space.norm  NonBlockingHashMap  thrpt               0.036            B/op
 * MpmcMapBenchmark.workload:·gc.count                         NonBlockingHashMap  thrpt               8.000          counts
 * MpmcMapBenchmark.workload:·gc.time                          NonBlockingHashMap  thrpt             339.000              ms
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@State(Scope.Benchmark)
public class MpmcMapBenchmark {

  public enum Impl {
    Noop, ConcurrentHashMap, NonBlockingHashMap, CustomEntryConcurrentHashMap
  }

  @Param
  public Impl impl;
  private Map<Object, Object> map;

  @Setup
  public void setup() {
    switch (impl) {
      case Noop:
        map = new Noop<>();
        break;
      case ConcurrentHashMap:
        map = new ConcurrentHashMap<>();
        break;
      case NonBlockingHashMap:
        map = new NonBlockingHashMap<>();
        break;
      case CustomEntryConcurrentHashMap:
        map = new CustomEntryConcurrentHashMap<>();
        break;
    }

    ObjectSizeProfiler.objectSize("map", map);
  }

  @TearDown
  public void tearDown() {}

  @Benchmark
  public void workload() {
    Object key = randomKey();
    Object value = map.get(key);
    if (null == value) {
      value = new Object();
    }
    consumeCPU(10);
    map.put(key, value);
  }

  private Object randomKey() {
    return ThreadLocalRandom.current().nextInt(0, 1_000_000);
  }

  private static class Noop<K, V> implements Map<K, V> {
    @Override
    public int size() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean containsKey(final Object key) {
      return false;
    }

    @Override
    public boolean containsValue(final Object value) {
      return false;
    }

    @Override
    public V get(final Object key) {
      return null;
    }

    @Nullable
    @Override
    public V put(final K key, final V value) {
      return null;
    }

    @Override
    public V remove(final Object key) {
      return null;
    }

    @Override
    public void putAll(@NotNull final Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {

    }

    @NotNull
    @Override
    public Set<K> keySet() {
      return null;
    }

    @NotNull
    @Override
    public Collection<V> values() {
      return null;
    }

    @NotNull
    @Override
    public Set<Entry<K, V>> entrySet() {
      return null;
    }
  }

}
