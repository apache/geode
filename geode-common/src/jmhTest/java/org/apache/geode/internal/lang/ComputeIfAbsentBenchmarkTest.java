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

import static org.apache.geode.internal.lang.ComputeIfAbsentBenchmark.Impl.direct;
import static org.apache.geode.internal.lang.ComputeIfAbsentBenchmark.Impl.noop;
import static org.apache.geode.internal.lang.ComputeIfAbsentBenchmark.Impl.workaround;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ComputeIfAbsentBenchmarkTest {

  private final ComputeIfAbsentBenchmark benchmark = new ComputeIfAbsentBenchmark();

  @Test
  public void setupForNoop() {
    benchmark.impl = noop;
    benchmark.setup();
    assertThat(benchmark.accessor).isSameAs(benchmark.noopAccessor);
    assertThat(benchmark.accessor.apply(1)).isEqualTo(1);
    assertThat(benchmark.accessor.apply(2)).isEqualTo(2);
    assertThat(benchmark.map).isEmpty();
  }

  @Test
  public void setupForDirect() {
    benchmark.impl = direct;
    benchmark.setup();
    assertThat(benchmark.accessor).isSameAs(benchmark.directAccessor);
    assertThat(benchmark.accessor.apply(1)).isEqualTo(1);
    assertThat(benchmark.accessor.apply(2)).isEqualTo(2);
    assertThat(benchmark.map).containsOnlyKeys(1, 2);
  }

  @Test
  public void setupForWorkaround() {
    benchmark.impl = workaround;
    benchmark.setup();
    assertThat(benchmark.accessor).isSameAs(benchmark.workaroundAccessor);
    assertThat(benchmark.accessor.apply(1)).isEqualTo(1);
    assertThat(benchmark.accessor.apply(2)).isEqualTo(2);
    assertThat(benchmark.map).containsOnlyKeys(1, 2);
  }
}
