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
