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
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.LinkedBlockingDeque;

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

import org.apache.geode.benchmark.jmh.profilers.ObjectSizeProfiler;

/**
 * Benchmark multi-producer multi-consumer LIFO queues.
 * <p>
 * Example:
 *
 * <pre>
 * $ ./gradlew geode-core:jmh -Pjmh.include=MpmcLifoQueueBenchmark \
 *     -Pjmh.profilers="org.apache.geode.benchmark.jmh.profilers.ObjectSizeProfiler gc" \
 *     -Pjmh.forks=5 -Pjmh.iterations=5 -Pjmh.warmupIterations=5 -Pjmh.threads=12
 *
 * Benchmark                                                            (impl)   Mode  Cnt          Score          Error   Units
 * MpmcLifoQueueBenchmark.workload                                        Noop  thrpt    5  372691420.242 ± 21218737.205   ops/s
 * MpmcLifoQueueBenchmark.workload:objectSize.queue                       Noop  thrpt    5         16.000 ±        0.001   bytes
 * MpmcLifoQueueBenchmark.workload:·gc.alloc.rate                         Noop  thrpt    5          0.053 ±        0.115  MB/sec
 * MpmcLifoQueueBenchmark.workload:·gc.alloc.rate.norm                    Noop  thrpt    5         ≈ 10⁻⁴                   B/op
 * MpmcLifoQueueBenchmark.workload:·gc.count                              Noop  thrpt    5            ≈ 0                 counts
 * MpmcLifoQueueBenchmark.workload                       ConcurrentLinkedDeque  thrpt    5    4332596.744 ±  1085091.820   ops/s
 * MpmcLifoQueueBenchmark.workload:objectSize.queue      ConcurrentLinkedDeque  thrpt    5        888.000 ±      175.616   bytes
 * MpmcLifoQueueBenchmark.workload:·gc.alloc.rate        ConcurrentLinkedDeque  thrpt    5        157.347 ±       39.354  MB/sec
 * MpmcLifoQueueBenchmark.workload:·gc.alloc.rate.norm   ConcurrentLinkedDeque  thrpt    5         40.002 ±        0.001    B/op
 * MpmcLifoQueueBenchmark.workload:·gc.churn.ZHeap       ConcurrentLinkedDeque  thrpt    5        157.373 ±        2.387  MB/sec
 * MpmcLifoQueueBenchmark.workload:·gc.churn.ZHeap.norm  ConcurrentLinkedDeque  thrpt    5         40.148 ±       10.370    B/op
 * MpmcLifoQueueBenchmark.workload:·gc.count             ConcurrentLinkedDeque  thrpt    5         10.000                 counts
 * MpmcLifoQueueBenchmark.workload:·gc.time              ConcurrentLinkedDeque  thrpt    5        502.000                     ms
 * MpmcLifoQueueBenchmark.workload                         LinkedBlockingDeque  thrpt    5   14123974.138 ±   903240.794   ops/s
 * MpmcLifoQueueBenchmark.workload:objectSize.queue        LinkedBlockingDeque  thrpt    5        920.000 ±        0.001   bytes
 * MpmcLifoQueueBenchmark.workload:·gc.alloc.rate          LinkedBlockingDeque  thrpt    5        519.688 ±       32.848  MB/sec
 * MpmcLifoQueueBenchmark.workload:·gc.alloc.rate.norm     LinkedBlockingDeque  thrpt    5         40.544 ±        0.050    B/op
 * MpmcLifoQueueBenchmark.workload:·gc.churn.ZHeap         LinkedBlockingDeque  thrpt    5        515.245 ±      166.265  MB/sec
 * MpmcLifoQueueBenchmark.workload:·gc.churn.ZHeap.norm    LinkedBlockingDeque  thrpt    5         40.185 ±       11.904    B/op
 * MpmcLifoQueueBenchmark.workload:·gc.count               LinkedBlockingDeque  thrpt    5         32.000                 counts
 * MpmcLifoQueueBenchmark.workload:·gc.time                LinkedBlockingDeque  thrpt    5        248.000                     ms
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@State(Scope.Benchmark)
public class MpmcLifoQueueBenchmark {

  public enum Impl {
    Noop, ConcurrentLinkedDeque, LinkedBlockingDeque
  }

  @Param
  public Impl impl;
  private Deque<Object> queue;

  @Setup
  public void setup() {
    switch (impl) {
      case Noop:
        queue = new Noop<>();
        break;
      case ConcurrentLinkedDeque:
        queue = new ConcurrentLinkedDeque<>();
        break;
      case LinkedBlockingDeque:
        queue = new LinkedBlockingDeque<>(1000);
        break;
    }

    ObjectSizeProfiler.objectSize("queue", queue);
  }

  @TearDown
  public void tearDown() {}

  @Benchmark
  public void workload() {
    Object o = queue.pollFirst();
    if (null == o) {
      o = new Object();
    }
    consumeCPU(10);
    queue.offerFirst(o);
  }

  private static class Noop<E> implements Deque<E> {
    @Override
    public void addFirst(final E e) {

    }

    @Override
    public void addLast(final E e) {

    }

    @Override
    public boolean offerFirst(final E e) {
      return false;
    }

    @Override
    public boolean offerLast(final E e) {
      return false;
    }

    @Override
    public E removeFirst() {
      return null;
    }

    @Override
    public E removeLast() {
      return null;
    }

    @Override
    public E pollFirst() {
      return null;
    }

    @Override
    public E pollLast() {
      return null;
    }

    @Override
    public E getFirst() {
      return null;
    }

    @Override
    public E getLast() {
      return null;
    }

    @Override
    public E peekFirst() {
      return null;
    }

    @Override
    public E peekLast() {
      return null;
    }

    @Override
    public boolean removeFirstOccurrence(final Object o) {
      return false;
    }

    @Override
    public boolean removeLastOccurrence(final Object o) {
      return false;
    }

    @Override
    public boolean add(final E e) {
      return false;
    }

    @Override
    public boolean offer(final E e) {
      return false;
    }

    @Override
    public E remove() {
      return null;
    }

    @Override
    public E poll() {
      return null;
    }

    @Override
    public E element() {
      return null;
    }

    @Override
    public E peek() {
      return null;
    }

    @Override
    public void push(final E e) {

    }

    @Override
    public E pop() {
      return null;
    }

    @Override
    public boolean remove(final Object o) {
      return false;
    }

    @Override
    public boolean contains(final Object o) {
      return false;
    }

    @Override
    public int size() {
      return 0;
    }

    @NotNull
    @Override
    public Iterator<E> iterator() {
      return Collections.emptyIterator();
    }

    @NotNull
    @Override
    public Iterator<E> descendingIterator() {
      return Collections.emptyIterator();
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @NotNull
    @Override
    public Object @NotNull [] toArray() {
      return new Object[0];
    }

    @SuppressWarnings("unchecked")
    @NotNull
    @Override
    public <T> T @NotNull [] toArray(@NotNull final T @NotNull [] a) {
      return (T[]) new Object[0];
    }

    @Override
    public boolean containsAll(@NotNull final Collection<?> c) {
      return false;
    }

    @Override
    public boolean addAll(@NotNull final Collection<? extends E> c) {
      return false;
    }

    @Override
    public boolean removeAll(@NotNull final Collection<?> c) {
      return false;
    }

    @Override
    public boolean retainAll(@NotNull final Collection<?> c) {
      return false;
    }

    @Override
    public void clear() {

    }
  }

}
