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
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

import org.jctools.queues.MpmcArrayQueue;
import org.jctools.queues.MpmcUnboundedXaddArrayQueue;
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
 * Benchmark multi-producer multi-consumer FIFO queues.
 *
 * Example:
 *
 * <pre>
 * Benchmark                                                                              (impl)   Mode  Cnt          Score   Error   Units
 * MpmcFifoQueueBenchmark.workload                                                          Noop  thrpt       459948597.795           ops/s
 * MpmcFifoQueueBenchmark.workload:objectSize.queue                                         Noop  thrpt              16.000           bytes
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate                                           Noop  thrpt               0.635          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate.norm                                      Noop  thrpt               0.002            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.count                                                Noop  thrpt                 ≈ 0          counts
 * MpmcFifoQueueBenchmark.workload                                         ConcurrentLinkedDeque  thrpt         6186028.338           ops/s
 * MpmcFifoQueueBenchmark.workload:objectSize.queue                        ConcurrentLinkedDeque  thrpt             552.000           bytes
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate                          ConcurrentLinkedDeque  thrpt             134.818          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate.norm                     ConcurrentLinkedDeque  thrpt              24.001            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Eden_Space                 ConcurrentLinkedDeque  thrpt             145.271          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Eden_Space.norm            ConcurrentLinkedDeque  thrpt              25.862            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Survivor_Space             ConcurrentLinkedDeque  thrpt               0.054          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Survivor_Space.norm        ConcurrentLinkedDeque  thrpt               0.010            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.count                               ConcurrentLinkedDeque  thrpt              16.000          counts
 * MpmcFifoQueueBenchmark.workload:·gc.time                                ConcurrentLinkedDeque  thrpt               9.000              ms
 * MpmcFifoQueueBenchmark.workload                                         ConcurrentLinkedQueue  thrpt         6794496.691           ops/s
 * MpmcFifoQueueBenchmark.workload:objectSize.queue                        ConcurrentLinkedQueue  thrpt             528.000           bytes
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate                          ConcurrentLinkedQueue  thrpt             148.102          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate.norm                     ConcurrentLinkedQueue  thrpt              24.002            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Eden_Space                 ConcurrentLinkedQueue  thrpt             154.695          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Eden_Space.norm            ConcurrentLinkedQueue  thrpt              25.070            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Survivor_Space             ConcurrentLinkedQueue  thrpt               0.059          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Survivor_Space.norm        ConcurrentLinkedQueue  thrpt               0.010            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.count                               ConcurrentLinkedQueue  thrpt              18.000          counts
 * MpmcFifoQueueBenchmark.workload:·gc.time                                ConcurrentLinkedQueue  thrpt              11.000              ms
 * MpmcFifoQueueBenchmark.workload                                           LinkedBlockingQueue  thrpt         7622462.715           ops/s
 * MpmcFifoQueueBenchmark.workload:objectSize.queue                          LinkedBlockingQueue  thrpt             776.000           bytes
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate                            LinkedBlockingQueue  thrpt             173.714          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate.norm                       LinkedBlockingQueue  thrpt              25.109            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Eden_Space                   LinkedBlockingQueue  thrpt             177.161          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Eden_Space.norm              LinkedBlockingQueue  thrpt              25.607            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Survivor_Space               LinkedBlockingQueue  thrpt               0.048          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Survivor_Space.norm          LinkedBlockingQueue  thrpt               0.007            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.count                                 LinkedBlockingQueue  thrpt              22.000          counts
 * MpmcFifoQueueBenchmark.workload:·gc.time                                  LinkedBlockingQueue  thrpt              12.000              ms
 * MpmcFifoQueueBenchmark.workload                                           LinkedBlockingDeque  thrpt        12832817.706           ops/s
 * MpmcFifoQueueBenchmark.workload:objectSize.queue                          LinkedBlockingDeque  thrpt             648.000           bytes
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate                            LinkedBlockingDeque  thrpt             283.971          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate.norm                       LinkedBlockingDeque  thrpt              24.376            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Eden_Space                   LinkedBlockingDeque  thrpt             284.990          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Eden_Space.norm              LinkedBlockingDeque  thrpt              24.464            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Survivor_Space               LinkedBlockingDeque  thrpt               0.086          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.churn.PS_Survivor_Space.norm          LinkedBlockingDeque  thrpt               0.007            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.count                                 LinkedBlockingDeque  thrpt              53.000          counts
 * MpmcFifoQueueBenchmark.workload:·gc.time                                  LinkedBlockingDeque  thrpt              24.000              ms
 * MpmcFifoQueueBenchmark.workload                                            ArrayBlockingQueue  thrpt        12479059.530           ops/s
 * MpmcFifoQueueBenchmark.workload:objectSize.queue                           ArrayBlockingQueue  thrpt            4384.000           bytes
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate                             ArrayBlockingQueue  thrpt               4.085          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate.norm                        ArrayBlockingQueue  thrpt               0.361            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.count                                  ArrayBlockingQueue  thrpt                 ≈ 0          counts
 * MpmcFifoQueueBenchmark.workload                                                MpmcArrayQueue  thrpt         5436780.755           ops/s
 * MpmcFifoQueueBenchmark.workload:objectSize.queue                               MpmcArrayQueue  thrpt             560.000           bytes
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate                                 MpmcArrayQueue  thrpt               0.006          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate.norm                            MpmcArrayQueue  thrpt               0.001            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.count                                      MpmcArrayQueue  thrpt                 ≈ 0          counts
 * MpmcFifoQueueBenchmark.workload                                   MpmcUnboundedXaddArrayQueue  thrpt         5466111.269           ops/s
 * MpmcFifoQueueBenchmark.workload:objectSize.queue                  MpmcUnboundedXaddArrayQueue  thrpt             552.000           bytes
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate                    MpmcUnboundedXaddArrayQueue  thrpt               0.001          MB/sec
 * MpmcFifoQueueBenchmark.workload:·gc.alloc.rate.norm               MpmcUnboundedXaddArrayQueue  thrpt              ≈ 10⁻⁴            B/op
 * MpmcFifoQueueBenchmark.workload:·gc.count                         MpmcUnboundedXaddArrayQueue  thrpt                 ≈ 0          counts
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@State(Scope.Benchmark)
public class MpmcFifoQueueBenchmark {

  public enum Impl {
    Noop,
    ConcurrentLinkedDeque,
    ConcurrentLinkedQueue,
    LinkedBlockingQueue,
    LinkedBlockingDeque,
    ArrayBlockingQueue,
    MpmcArrayQueue,
    MpmcUnboundedXaddArrayQueue
  }

  @Param
  public Impl impl;
  private Queue<Object> queue;

  @Setup
  public void setup() {
    switch (impl) {
      case Noop:
        queue = new Noop<>();
        break;
      case ConcurrentLinkedDeque:
        queue = new ConcurrentLinkedDeque<>();
        break;
      case ConcurrentLinkedQueue:
        queue = new ConcurrentLinkedQueue<>();
        break;
      case LinkedBlockingQueue:
        queue = new LinkedBlockingQueue<>(1000);
        break;
      case LinkedBlockingDeque:
        queue = new LinkedBlockingDeque<>(1000);
        break;
      case ArrayBlockingQueue:
        queue = new ArrayBlockingQueue<>(1000);
        break;
      case MpmcArrayQueue:
        queue = new MpmcArrayQueue<>(1000);
        break;
      case MpmcUnboundedXaddArrayQueue:
        queue = new MpmcUnboundedXaddArrayQueue<>(1000);
        break;
    }

    ObjectSizeProfiler.objectSize("queue", queue);
  }

  @TearDown
  public void tearDown() {}

  @Benchmark
  public void workload() {
    Object o = queue.poll();
    if (null == o) {
      o = new Object();
    }
    consumeCPU(10);
    queue.offer(o);
  }

  private static class Noop<E> implements Queue<E> {

    @Override
    public int size() {
      return 0;
    }

    @Override
    public boolean isEmpty() {
      return false;
    }

    @Override
    public boolean contains(final Object o) {
      return false;
    }

    @NotNull
    @Override
    public Iterator<E> iterator() {
      return null;
    }

    @NotNull
    @Override
    public Object[] toArray() {
      return new Object[0];
    }

    @NotNull
    @Override
    public <T> T[] toArray(@NotNull final T[] a) {
      return null;
    }

    @Override
    public boolean add(final E e) {
      return false;
    }

    @Override
    public boolean remove(final Object o) {
      return false;
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
  }

}
