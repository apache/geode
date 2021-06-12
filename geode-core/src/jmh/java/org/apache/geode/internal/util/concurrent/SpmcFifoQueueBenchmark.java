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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.openjdk.jmh.infra.Blackhole.consumeCPU;

import java.util.Collections;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.jctools.queues.SpmcArrayQueue;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.GroupThreads;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Control;

import org.apache.geode.benchmark.jmh.profilers.ObjectSizeProfiler;

/**
 * Benchmark simple-producer multi-consumer FIFO queues.
 * <p>
 * Example:
 *
 * <pre>
 * Benchmark                                                      (impl)   Mode  Cnt         Score   Error  Units
 * SpmcFifoQueueBenchmark.blocking                                  Noop  thrpt       80003104.521          ops/s
 * SpmcFifoQueueBenchmark.blocking:blockingConsumer                 Noop  thrpt       46020083.428          ops/s
 * SpmcFifoQueueBenchmark.blocking:blockingProducer                 Noop  thrpt       33983021.094          ops/s
 * SpmcFifoQueueBenchmark.blocking                   LinkedBlockingQueue  thrpt        3850647.391          ops/s
 * SpmcFifoQueueBenchmark.blocking:blockingConsumer  LinkedBlockingQueue  thrpt        1925311.886          ops/s
 * SpmcFifoQueueBenchmark.blocking:blockingProducer  LinkedBlockingQueue  thrpt        1925335.505          ops/s
 * SpmcFifoQueueBenchmark.blocking                   LinkedBlockingDeque  thrpt        3481534.299          ops/s
 * SpmcFifoQueueBenchmark.blocking:blockingConsumer  LinkedBlockingDeque  thrpt        1740735.382          ops/s
 * SpmcFifoQueueBenchmark.blocking:blockingProducer  LinkedBlockingDeque  thrpt        1740798.917          ops/s
 * SpmcFifoQueueBenchmark.blocking                    ArrayBlockingQueue  thrpt        3274995.095          ops/s
 * SpmcFifoQueueBenchmark.blocking:blockingConsumer   ArrayBlockingQueue  thrpt        1637522.865          ops/s
 * SpmcFifoQueueBenchmark.blocking:blockingProducer   ArrayBlockingQueue  thrpt        1637472.230          ops/s
 * SpmcFifoQueueBenchmark.blocking                        SpmcArrayQueue  thrpt       11241498.949          ops/s
 * SpmcFifoQueueBenchmark.blocking:blockingConsumer       SpmcArrayQueue  thrpt        5620759.618          ops/s
 * SpmcFifoQueueBenchmark.blocking:blockingProducer       SpmcArrayQueue  thrpt        5620739.331          ops/s
 * SpmcFifoQueueBenchmark.nonblocking                               Noop  thrpt       71738429.129          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:consumer                      Noop  thrpt       41626782.666          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:offerFailures                 Noop  thrpt                ≈ 0          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:pollFailures                  Noop  thrpt                ≈ 0          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:producer                      Noop  thrpt       30111646.463          ops/s
 * SpmcFifoQueueBenchmark.nonblocking                LinkedBlockingQueue  thrpt        4068996.705          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:consumer       LinkedBlockingQueue  thrpt        2034500.018          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:offerFailures  LinkedBlockingQueue  thrpt       83206795.820          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:pollFailures   LinkedBlockingQueue  thrpt          21473.525          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:producer       LinkedBlockingQueue  thrpt        2034496.687          ops/s
 * SpmcFifoQueueBenchmark.nonblocking                LinkedBlockingDeque  thrpt        3086720.270          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:consumer       LinkedBlockingDeque  thrpt        1543373.209          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:offerFailures  LinkedBlockingDeque  thrpt           1214.822          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:pollFailures   LinkedBlockingDeque  thrpt       16600021.618          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:producer       LinkedBlockingDeque  thrpt        1543347.060          ops/s
 * SpmcFifoQueueBenchmark.nonblocking                 ArrayBlockingQueue  thrpt        2953571.178          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:consumer        ArrayBlockingQueue  thrpt        1476800.612          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:offerFailures   ArrayBlockingQueue  thrpt            155.199          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:pollFailures    ArrayBlockingQueue  thrpt       17007128.409          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:producer        ArrayBlockingQueue  thrpt        1476770.566          ops/s
 * SpmcFifoQueueBenchmark.nonblocking                     SpmcArrayQueue  thrpt       15686253.905          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:consumer            SpmcArrayQueue  thrpt        7843104.733          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:offerFailures       SpmcArrayQueue  thrpt         168362.673          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:pollFailures        SpmcArrayQueue  thrpt       22818322.784          ops/s
 * SpmcFifoQueueBenchmark.nonblocking:producer            SpmcArrayQueue  thrpt        7843149.173          ops/s
 * </pre>
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@State(Scope.Benchmark)
public class SpmcFifoQueueBenchmark {

  public enum Impl {
    Noop,
    LinkedBlockingQueue,
    LinkedBlockingDeque,
    ArrayBlockingQueue,
    SpmcArrayQueue
  }

  @Param({"Noop",
      "LinkedBlockingQueue",
      "LinkedBlockingDeque",
      "ArrayBlockingQueue",
      "SpmcArrayQueue"})
  public Impl impl;
  private Accessor accessor;

  @State(Scope.Thread)
  @AuxCounters(AuxCounters.Type.OPERATIONS)
  public static class OpCounters {
    public int pollFailures;
    public int offerFailures;
  }

  @Setup
  public void setup() {
    switch (impl) {
      case Noop:
        accessor = new NoopAccessor();
        break;
      case LinkedBlockingQueue:
        accessor = new BlockingQueueAccessor(new LinkedBlockingQueue<>(1000));
        break;
      case LinkedBlockingDeque:
        accessor = new BlockingQueueAccessor(new LinkedBlockingDeque<>(1000));
        break;
      case ArrayBlockingQueue:
        accessor = new BlockingQueueAccessor(new ArrayBlockingQueue<>(1000));
        break;
      case SpmcArrayQueue:
        accessor = new SpmcArrayQueueAccessor(1000);
        break;
      default:
        throw new IllegalStateException();
    }

    ObjectSizeProfiler.objectSize("queue", accessor.getQueue());
  }

  @Benchmark
  @Group("nonblocking")
  @GroupThreads(1)
  public void producer(final Control control, final OpCounters opCounters) {
    simulateProducerWorkload();
    final Object o = new Object();
    while (!accessor.offer(o)) {
      if (control.stopMeasurement) {
        return;
      }
      opCounters.offerFailures++;
    }
  }

  @Benchmark
  @Group("nonblocking")
  @GroupThreads(11)
  public Object consumer(final Control control, final OpCounters opCounters) {
    Object o;
    while ((o = accessor.poll()) == null) {
      if (control.stopMeasurement) {
        return null;
      }
      opCounters.pollFailures++;
    }
    simulateConsumerWorkload();
    return o;
  }

  @Benchmark
  @Group("blocking")
  @GroupThreads(1)
  public void blockingProducer() throws InterruptedException {
    simulateProducerWorkload();
    accessor.offer(new Object(), 1, SECONDS);
  }

  @Benchmark
  @Group("blocking")
  @GroupThreads(11)
  public Object blockingConsumer()
      throws InterruptedException {
    final Object o = accessor.poll(1, SECONDS);
    simulateConsumerWorkload();
    return o;
  }

  private void simulateProducerWorkload() {
    consumeCPU(10);
  }

  private void simulateConsumerWorkload() {
    consumeCPU(100);
  }

  private interface Accessor {
    boolean offer(@NotNull Object value);

    Object poll();

    boolean offer(@NotNull Object value, long timeout, TimeUnit timeUnit)
        throws InterruptedException;

    Object poll(long timeout, TimeUnit timeUnit) throws InterruptedException;

    Object getQueue();
  }

  private static class NoopAccessor implements Accessor {
    final Object value = new Object();

    @Override
    public boolean offer(@NotNull final Object value) {
      return true;
    }

    @Override
    public Object poll() {
      return value;
    }

    @Override
    public boolean offer(final @NotNull Object value, long timeout, TimeUnit timeUnit) {
      return true;
    }

    @Override
    public Object poll(long timeout, TimeUnit timeUnit) {
      return value;
    }

    @Override
    public @NotNull Object getQueue() {
      return Collections.emptyList();
    }
  }

  private static class BlockingQueueAccessor implements Accessor {
    private final BlockingQueue<Object> queue;

    public BlockingQueueAccessor(final BlockingQueue<Object> queue) {
      this.queue = queue;
    }

    @Override
    public boolean offer(@NotNull final Object value) {
      return queue.offer(value);
    }

    @Override
    public Object poll() {
      return queue.poll();
    }

    @Override
    public boolean offer(@NotNull final Object value, long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      return queue.offer(value, timeout, timeUnit);
    }

    @Override
    public Object poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
      return queue.poll(1, SECONDS);
    }

    @Override
    public BlockingQueue<Object> getQueue() {
      return queue;
    }
  }

  private static class SpmcArrayQueueAccessor implements Accessor {
    private final SpmcArrayQueue<Object> queue;

    public SpmcArrayQueueAccessor(final int capacity) {
      queue = new SpmcArrayQueue<>(capacity);
    }

    @Override
    public boolean offer(@NotNull final Object value) {
      return queue.offer(value);
    }

    @Override
    public Object poll() {
      return queue.poll();
    }

    @Override
    public boolean offer(@NotNull final Object value, long timeout, TimeUnit timeUnit)
        throws InterruptedException {
      final long expires = now() + timeUnit.toNanos(timeout);
      while (!offer(value)) {
        if (now() > expires) {
          return false;
        }
        waitStrategy();
      }
      return true;
    }

    @Override
    public Object poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
      final long expires = now() + timeUnit.toNanos(timeout);
      Object value;
      while ((value = poll()) == null) {
        if (now() > expires) {
          return null;
        }
        waitStrategy();
      }
      return value;
    }

    private long now() {
      return System.nanoTime();
    }

    private void waitStrategy() throws InterruptedException {
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
      Thread.yield();
    }

    @Override
    public SpmcArrayQueue<Object> getQueue() {
      return queue;
    }
  }
}
