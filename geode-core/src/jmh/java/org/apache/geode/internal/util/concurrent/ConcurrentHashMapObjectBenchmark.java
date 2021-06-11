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

import static org.apache.geode.internal.lang.utils.JavaWorkarounds.computeIfAbsent;
import static org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.DEFAULT_CONCURRENCY_LEVEL;
import static org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.DEFAULT_INITIAL_CAPACITY;
import static org.apache.geode.internal.util.concurrent.CustomEntryConcurrentHashMap.DEFAULT_LOAD_FACTOR;
import static org.apache.geode.util.internal.UncheckedUtils.uncheckedCast;
import static org.openjdk.jmh.infra.Blackhole.consumeCPU;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

import org.jctools.maps.NonBlockingHashMap;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import org.apache.geode.benchmark.jmh.profilers.ObjectSizeProfiler;
import org.apache.geode.internal.cache.RegionClearedException;
import org.apache.geode.internal.cache.RegionEntry;
import org.apache.geode.internal.cache.RegionEntryContext;
import org.apache.geode.internal.cache.RegionEntryFactory;
import org.apache.geode.internal.cache.Token;
import org.apache.geode.internal.cache.entries.AbstractRegionEntry;
import org.apache.geode.internal.cache.entries.VMThinRegionEntryHeapObjectKey;
import org.apache.geode.internal.cache.versions.VersionStamp;

/**
 * Benchmarks various hash maps.
 *
 * <p>
 * Example:
 *
 * <pre>
 * $ ./gradlew geode-core:jmh -Pjmh.include=ConcurrentHashMapObjectBenchmark -Pjmh.threads=12 -Pjmh.forks=5 -Pjmh.iterations=5 -Pjmh.warmupIterations=5 \
 *     -Pjmh.profilers="org.apache.geode.benchmark.jmh.profilers.ObjectSizeProfiler gc" \
 *     -Pjmh.jvmArgs="-XX:+UseZGC --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.util.concurrent.locks=ALL-UNNAMED --add-opens java.base/java.util.concurrent=ALL-UNNAMED"
 *
 * Benchmark                                                                               (impl)  (keyCount)   Mode  Cnt          Score          Error   Units
 * ConcurrentHashMapObjectBenchmark.workload                                                 Noop     1000000  thrpt    5   81523558.714 ± 13644275.635   ops/s
 * ConcurrentHashMapObjectBenchmark.workload:objectSize.map                                  Noop     1000000  thrpt    5         16.000 ±        0.001   bytes
 * ConcurrentHashMapObjectBenchmark.workload:·gc.alloc.rate                                  Noop     1000000  thrpt    5       2368.549 ±      395.713  MB/sec
 * ConcurrentHashMapObjectBenchmark.workload:·gc.alloc.rate.norm                             Noop     1000000  thrpt    5         32.000 ±        0.001    B/op
 * ConcurrentHashMapObjectBenchmark.workload:·gc.churn.ZHeap                                 Noop     1000000  thrpt    5        472.307 ±      154.108  MB/sec
 * ConcurrentHashMapObjectBenchmark.workload:·gc.churn.ZHeap.norm                            Noop     1000000  thrpt    5          6.374 ±        1.257    B/op
 * ConcurrentHashMapObjectBenchmark.workload:·gc.count                                       Noop     1000000  thrpt    5         42.000                 counts
 * ConcurrentHashMapObjectBenchmark.workload:·gc.time                                        Noop     1000000  thrpt    5      47163.000                     ms
 * ConcurrentHashMapObjectBenchmark.workload                         CustomEntryConcurrentHashMap     1000000  thrpt    5   11170779.856 ±   344570.190   ops/s
 * ConcurrentHashMapObjectBenchmark.workload:objectSize.map          CustomEntryConcurrentHashMap     1000000  thrpt    5  112784376.000 ±        0.001   bytes
 * ConcurrentHashMapObjectBenchmark.workload:·gc.alloc.rate          CustomEntryConcurrentHashMap     1000000  thrpt    5       1643.523 ±       48.218  MB/sec
 * ConcurrentHashMapObjectBenchmark.workload:·gc.alloc.rate.norm     CustomEntryConcurrentHashMap     1000000  thrpt    5        161.347 ±        1.639    B/op
 * ConcurrentHashMapObjectBenchmark.workload:·gc.churn.ZHeap         CustomEntryConcurrentHashMap     1000000  thrpt    5       1058.866 ±      105.839  MB/sec
 * ConcurrentHashMapObjectBenchmark.workload:·gc.churn.ZHeap.norm    CustomEntryConcurrentHashMap     1000000  thrpt    5        103.938 ±        7.109    B/op
 * ConcurrentHashMapObjectBenchmark.workload:·gc.count               CustomEntryConcurrentHashMap     1000000  thrpt    5         29.000                 counts
 * ConcurrentHashMapObjectBenchmark.workload:·gc.time                CustomEntryConcurrentHashMap     1000000  thrpt    5      49613.000                     ms
 * ConcurrentHashMapObjectBenchmark.workload                       VMThinRegionEntryHeapObjectKey     1000000  thrpt    5   10811820.191 ±  1424626.451   ops/s
 * ConcurrentHashMapObjectBenchmark.workload:objectSize.map        VMThinRegionEntryHeapObjectKey     1000000  thrpt    5   80784376.000 ±        0.001   bytes
 * ConcurrentHashMapObjectBenchmark.workload:·gc.alloc.rate        VMThinRegionEntryHeapObjectKey     1000000  thrpt    5       1591.383 ±      205.191  MB/sec
 * ConcurrentHashMapObjectBenchmark.workload:·gc.alloc.rate.norm   VMThinRegionEntryHeapObjectKey     1000000  thrpt    5        161.774 ±        1.463    B/op
 * ConcurrentHashMapObjectBenchmark.workload:·gc.churn.ZHeap       VMThinRegionEntryHeapObjectKey     1000000  thrpt    5        737.971 ±      139.400  MB/sec
 * ConcurrentHashMapObjectBenchmark.workload:·gc.churn.ZHeap.norm  VMThinRegionEntryHeapObjectKey     1000000  thrpt    5         75.049 ±       13.626    B/op
 * ConcurrentHashMapObjectBenchmark.workload:·gc.count             VMThinRegionEntryHeapObjectKey     1000000  thrpt    5         28.000                 counts
 * ConcurrentHashMapObjectBenchmark.workload:·gc.time              VMThinRegionEntryHeapObjectKey     1000000  thrpt    5      48952.000                     ms
 * ConcurrentHashMapObjectBenchmark.workload                                    ConcurrentHashMap     1000000  thrpt    5   23505481.296 ±   393929.840   ops/s
 * ConcurrentHashMapObjectBenchmark.workload:objectSize.map                     ConcurrentHashMap     1000000  thrpt    5  104777328.000 ±        0.001   bytes
 * ConcurrentHashMapObjectBenchmark.workload:·gc.alloc.rate                     ConcurrentHashMap     1000000  thrpt    5        512.460 ±        8.794  MB/sec
 * ConcurrentHashMapObjectBenchmark.workload:·gc.alloc.rate.norm                ConcurrentHashMap     1000000  thrpt    5         24.001 ±        0.001    B/op
 * ConcurrentHashMapObjectBenchmark.workload:·gc.churn.ZHeap                    ConcurrentHashMap     1000000  thrpt    5        235.158 ±       51.539  MB/sec
 * ConcurrentHashMapObjectBenchmark.workload:·gc.churn.ZHeap.norm               ConcurrentHashMap     1000000  thrpt    5         11.014 ±        2.415    B/op
 * ConcurrentHashMapObjectBenchmark.workload:·gc.count                          ConcurrentHashMap     1000000  thrpt    5         17.000                 counts
 * ConcurrentHashMapObjectBenchmark.workload:·gc.time                           ConcurrentHashMap     1000000  thrpt    5      45529.000                     ms
 * ConcurrentHashMapObjectBenchmark.workload                                   NonBlockingHashMap     1000000  thrpt    5   24229833.176 ±   297156.143   ops/s
 * ConcurrentHashMapObjectBenchmark.workload:objectSize.map                    NonBlockingHashMap     1000000  thrpt    5   89943856.000 ±        0.001   bytes
 * ConcurrentHashMapObjectBenchmark.workload:·gc.alloc.rate                    NonBlockingHashMap     1000000  thrpt    5        527.983 ±        6.688  MB/sec
 * ConcurrentHashMapObjectBenchmark.workload:·gc.alloc.rate.norm               NonBlockingHashMap     1000000  thrpt    5         24.001 ±        0.001    B/op
 * ConcurrentHashMapObjectBenchmark.workload:·gc.churn.ZHeap                   NonBlockingHashMap     1000000  thrpt    5        197.084 ±      336.599  MB/sec
 * ConcurrentHashMapObjectBenchmark.workload:·gc.churn.ZHeap.norm              NonBlockingHashMap     1000000  thrpt    5          8.963 ±       15.304    B/op
 * ConcurrentHashMapObjectBenchmark.workload:·gc.count                         NonBlockingHashMap     1000000  thrpt    5         10.000                 counts
 * ConcurrentHashMapObjectBenchmark.workload:·gc.time                          NonBlockingHashMap     1000000  thrpt    5      33799.000                     ms
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@State(Scope.Benchmark)
public class ConcurrentHashMapObjectBenchmark {

  @Param({/* "1", "10", "100", "1000", "10000", "100000", */
      "1000000" /* , "10000000", "100000000", "1000000000" */})
  public int keyCount;
  private Object[] keys;

  public enum Impl {
    /**
     * Does nothing. Used to detect benchmarks that are optimized out completely.
     */
    Noop,
    /**
     * Use {@link CustomEntryConcurrentHashMap}, which is a optimized version of {@link
     * ConcurrentHashMap} from JDK 1.5.
     */
    CustomEntryConcurrentHashMap,
    /**
     * Use {@link CustomEntryConcurrentHashMap} with {@link VMThinRegionEntryHeapObjectKey}, which
     * is similar to what is used to store Region entries.
     */
    VMThinRegionEntryHeapObjectKey,
    /**
     * Use {@link ConcurrentHashMap}.
     */
    ConcurrentHashMap,
    /**
     * Use {@link NonBlockingHashMap}.
     */
    NonBlockingHashMap
  }

  @Param
  public Impl impl;
  private Map<Object, RegionEntry> map;
  private RegionEntryFactory regionEntryFactory;

  @Setup
  public void setup() {
    regionEntryFactory = new SimpleRegionEntryFactory();
    switch (impl) {
      case Noop:
        map = new Noop<>();
        break;
      case CustomEntryConcurrentHashMap:
        map = new CustomEntryConcurrentHashMap<>();
        break;
      case VMThinRegionEntryHeapObjectKey:
        map = new CustomEntryConcurrentHashMap<>(DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR,
            DEFAULT_CONCURRENCY_LEVEL, false,
            uncheckedCast(new AbstractRegionEntry.HashRegionEntryCreator()));
        regionEntryFactory = VMThinRegionEntryHeapObjectKey.getEntryFactory();
        break;
      case ConcurrentHashMap:
        map = new ConcurrentHashMap<>();
        break;
      case NonBlockingHashMap:
        map = new NonBlockingHashMap<>();
        break;
      default:
        throw new IllegalStateException();
    }

    keys = new Object[keyCount];
    for (int i = 0; i < keys.length; i++) {
      keys[i] = new Object();
      try {
        put(keys[i]);
      } catch (RegionClearedException e) {
        throw new IllegalStateException(e);
      }
    }

    ObjectSizeProfiler.objectSize("map", map);
  }

  @TearDown
  public void tearDown() {}

  /**
   * Artificial work load of fetching a value from the cache, doing some work and replacing the
   * entry.
   */
  @Benchmark
  public RegionEntry workload(Blackhole blackhole) throws RegionClearedException {
    final Object key = getKey();
    final RegionEntry entry = map.get(key);
    blackhole.consume(entry);
    consumeCPU(10);
    return put(key);
  }

  @NotNull
  public RegionEntry getOrCreateRegionEntry(@NotNull Object key) {
    return computeIfAbsent(map, key,
        (k) -> regionEntryFactory.createEntry(null, k, Token.REMOVED_PHASE1));
  }

  @NotNull
  public final Object getKey() {
    return keys[ThreadLocalRandom.current().nextInt(0, keys.length)];
  }

  @NotNull
  public RegionEntry put(@NotNull final Object key) throws RegionClearedException {
    final RegionEntry regionEntry = getOrCreateRegionEntry(key);
    regionEntry.setValue(null, key);
    return regionEntry;
  }

  private static class Noop<K, V> implements Map<K, V> {
    @Override
    public int size() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return true;
    }

    @Override
    public boolean containsKey(Object key) {
      return false;
    }

    @Override
    public boolean containsValue(Object value) {
      return false;
    }

    @Override
    public V get(Object key) {
      return null;
    }

    @Override
    public V put(K key, V value) {
      return value;
    }

    @Override
    public V remove(Object key) {
      return null;
    }

    @Override
    public void putAll(@NotNull Map<? extends K, ? extends V> m) {

    }

    @Override
    public void clear() {

    }

    @Override
    @NotNull
    public Set<K> keySet() {
      return Collections.emptySet();
    }

    @Override
    @NotNull
    public Collection<V> values() {
      return Collections.emptySet();
    }

    @Override
    @NotNull
    public Set<Entry<K, V>> entrySet() {
      return Collections.emptySet();
    }
  }

  public static class SimpleRegionEntry extends AbstractRegionEntry {
    private Object value;
    @SuppressWarnings("unused") // here for like for like sizing
    private final long lastModified = 0L;

    protected SimpleRegionEntry(RegionEntryContext context, Object value) {
      super(context, value);
    }

    @Override
    public VersionStamp<?> getVersionStamp() {
      return null;
    }

    @Override
    protected Object getValueField() {
      return value;
    }

    @Override
    protected void setValueField(Object v) {
      value = v;
    }

    @Override
    public void setValue(RegionEntryContext context, Object value)
        throws RegionClearedException {
      super.setValue(context, value);
      if (value == Token.TOMBSTONE) {
        throw new RuntimeException("throw exception on setValue(TOMBSTONE)");
      }
    }

    @Override
    public int getEntryHash() {
      return 0;
    }

    @Override
    public CustomEntryConcurrentHashMap.HashEntry<Object, Object> getNextEntry() {
      return null;
    }

    @Override
    public void setNextEntry(CustomEntryConcurrentHashMap.HashEntry<Object, Object> n) {}

    @Override
    public Object getKey() {
      return null;
    }

    @Override
    protected long getLastModifiedField() {
      return 0;
    }

    @Override
    protected boolean compareAndSetLastModifiedField(long expectedValue, long newValue) {
      return false;
    }

    @Override
    protected void setEntryHash(int v) {}
  }

  private static class SimpleRegionEntryFactory implements RegionEntryFactory {
    @Override
    public RegionEntry createEntry(RegionEntryContext context, Object key, Object value) {
      return new SimpleRegionEntry(null, value);
    }

    @Override
    public Class<?> getEntryClass() {
      return null;
    }

    @Override
    public RegionEntryFactory makeVersioned() {
      return null;
    }

    @Override
    public RegionEntryFactory makeOnHeap() {
      return null;
    }
  }

}
