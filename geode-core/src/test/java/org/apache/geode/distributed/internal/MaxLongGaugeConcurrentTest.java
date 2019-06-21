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
package org.apache.geode.distributed.internal;

import static org.apache.geode.internal.statistics.StatisticDescriptorImpl.createLongGauge;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.internal.statistics.StatisticsTypeImpl;
import org.apache.geode.internal.statistics.StripedStatisticsImpl;
import org.apache.geode.test.concurrency.ConcurrentTestRunner;
import org.apache.geode.test.concurrency.ParallelExecutor;

@RunWith(ConcurrentTestRunner.class)
public class MaxLongGaugeConcurrentTest {
  private static final int PARALLEL_COUNT = 100;
  public static final int RECORDS_PER_TASK = 20;


  @Test
  public void recordMax(ParallelExecutor executor)
      throws Exception {
    StatisticDescriptor descriptor =
        createLongGauge("1", "", "", true);
    StatisticDescriptor[] descriptors = {descriptor};
    StatisticsTypeImpl statisticsType = new StatisticsTypeImpl("abc", "test",
        descriptors);
    StripedStatisticsImpl fakeStatistics = new StripedStatisticsImpl(
        statisticsType,
        "def", 12, 10,
        null);

    MaxLongGauge maxLongGauge = new MaxLongGauge(descriptor.getId(), fakeStatistics);
    ConcurrentLinkedQueue<Long> longs = new ConcurrentLinkedQueue<>();

    executor.inParallel(() -> {
      for (int i = 0; i < RECORDS_PER_TASK; i++) {
        long value = ThreadLocalRandom.current().nextLong();
        maxLongGauge.recordMax(value);
        longs.add(value);
      }
    }, PARALLEL_COUNT);
    executor.execute();

    long actualMax = fakeStatistics.getLong(descriptor.getId());
    long expectedMax = getMax(longs);

    assertThat(longs).hasSize(RECORDS_PER_TASK * PARALLEL_COUNT);
    assertThat(actualMax).isEqualTo(expectedMax);
  }

  private long getMax(ConcurrentLinkedQueue<Long> longs) {
    return Math.max(longs.parallelStream().max(Long::compareTo).get(), 0);
  }
}
