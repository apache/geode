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
package org.apache.geode.internal.util.redaction;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.geode.internal.util.ArgumentRedactor.redactEachInList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

@Measurement(iterations = 5, time = 5, timeUnit = MINUTES)
@Warmup(iterations = 1, time = 5, timeUnit = MINUTES)
@Fork(1)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(SECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused")
public class RedactEachInListBenchmark {

  private static final String SECRET_ARGUMENT = "s33kr1t";
  private static final String HYPHEN_D_PASSWORD =
      "-Dgemfire.password=" + SECRET_ARGUMENT;
  private static final String PASSWORD =
      "--password=" + SECRET_ARGUMENT;
  private static final String HYPHENS_J_D_PASSWORD =
      "--J=-Dgemfire.some.very.qualified.item.password=" + SECRET_ARGUMENT;
  private static final String HYPHENS_J_D_SYSPROP_HYPHEN =
      "--J=-Dsysprop-secret.information=" + SECRET_ARGUMENT;

  private List<String> someTabooOptions;
  private Blackhole blackhole;

  @Setup(Level.Trial)
  public void setUp() {
    someTabooOptions =
        asList(HYPHEN_D_PASSWORD, PASSWORD, HYPHENS_J_D_PASSWORD, HYPHENS_J_D_SYSPROP_HYPHEN);

    List<String> fullyRedacted = redactEachInList(someTabooOptions);

    assertThat(fullyRedacted)
        .doesNotContainAnyElementsOf(someTabooOptions);
  }

  @Benchmark
  public void redactEachInList_benchmark() {
    blackhole.consume(redactEachInList(someTabooOptions));
  }
}
