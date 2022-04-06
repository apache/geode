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

package org.apache.geode.internal;

import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Run this benchmark individually with:
 * <br/>
 * <code>
 *    ./gradlew -Pjmh.include=JvmSizeUtilsBenchmark geode-core:jmh
 * </code>
 */
@State(Scope.Thread)
@Fork(1)
public class JvmSizeUtilsBenchmark {

  private int i = 0;

  @TearDown(Level.Iteration)
  public void teardown() {
    i++;
  }

  @Benchmark
  @Measurement(iterations = 5)
  @Warmup(iterations = 3)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public long benchmark_roundUpSize() {
    return JvmSizeUtils.roundUpSize(i);
  }

  @Benchmark
  @Measurement(iterations = 5)
  @Warmup(iterations = 3)
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public long benchmark_old() {
    return roundUpSize_old(i);
  }

  /**
   * For reference, and to compare benchmark numbers, this is the original method.
   */
  private static long roundUpSize_old(long size) {
    long remainder = size % 8;
    if (remainder != 0) {
      size += 8 - remainder;
    }
    return size;
  }
}
