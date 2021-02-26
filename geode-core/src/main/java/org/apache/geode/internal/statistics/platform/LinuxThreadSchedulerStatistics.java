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

package org.apache.geode.internal.statistics.platform;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;

public class LinuxThreadSchedulerStatistics {

  // This is the only version of /proc/schedstat that we can parse (for now)
  public static final String SCHEDSTAT_FORMAT_VERSION = "15";
  public static final Path SCHEDSTAT_PATH = Paths.get("/proc/schedstat");

  private LinuxThreadSchedulerStatistics() {}

  /**
   * Create a new {@link Gatherer} for "/proc/schedstat"
   *
   * @return a {@link Gatherer} to be invoked when samples are needed
   */
  public static Gatherer create(final StatisticsFactory statisticsFactory,
      final StatisticsProducer statisticsProducer) {
    return create(statisticsFactory, statisticsProducer, SCHEDSTAT_PATH);
  }

  // visible for testing
  @NotNull
  static LinuxThreadSchedulerStatistics.Gatherer create(final StatisticsFactory statisticsFactory,
      final StatisticsProducer statisticsProducer,
      final Path path) {
    if (Files.exists(path)) {
      return new WorkingGatherer(statisticsFactory, statisticsProducer, path);
    } else {
      return () -> {
      }; // no-op
    }
  }

  @FunctionalInterface
  public interface StatisticsProducer {
    Statistics create(StatisticsType type, String textId);
  }

  @FunctionalInterface
  public interface Gatherer {
    void sample();
  }

  static class WorkingGatherer implements Gatherer {

    private static final String RUNNING_TIME_NANOS = "runningTimeNanos";
    private static final String QUEUED_TIME_NANOS = "queuedTimeNanos";
    private static final String TASKS_SCHEDULED_COUNT = "tasksScheduledCount";
    private static final String MEAN_TASK_QUEUED_TIME_NANOS = "meanTaskQueuedTimeNanos";

    // visible for testing
    final int runningTimeNanosId;
    final int queuedTimeNanosId;
    final int tasksScheduledCountId;
    final int meanTaskQueuedTimeNanosId;
    final HashMap<String, Statistics> statisticsForCpuIds = new HashMap<>();

    private final StatisticsType statisticsType;
    private final StatisticsProducer statisticsProducer;
    private final Path path;

    public WorkingGatherer(final StatisticsFactory statisticsFactory,
        final StatisticsProducer statisticsProducer,
        final Path path) {
      statisticsType =
          statisticsFactory.createType("LinuxThreadScheduler",
              "Per-CPU Linux scheduler statistics from /proc/schedstat",
              new StatisticDescriptor[] {
                  /*
                   * Descriptions come from
                   * http://eaglet.pdxhosts.com/rick/linux/schedstat/v15/format-15.html
                   * ...except the units are corrected here because of this change
                   * https://lkml.org/lkml/2019/7/24/906
                   */
                  statisticsFactory.createLongGauge(RUNNING_TIME_NANOS,
                      "sum of all time spent running by tasks on this processor", "nanoseconds"),
                  statisticsFactory.createLongGauge(QUEUED_TIME_NANOS,
                      "sum of all time spent waiting to run by tasks on this processor",
                      "nanoseconds"),
                  statisticsFactory.createLongGauge(TASKS_SCHEDULED_COUNT,
                      "# of tasks (not necessarily unique) given to the processor", "tasks"),
                  statisticsFactory.createDoubleGauge(MEAN_TASK_QUEUED_TIME_NANOS, "",
                      "nanoseconds/task")
              });

      runningTimeNanosId = statisticsType.nameToId(RUNNING_TIME_NANOS);
      queuedTimeNanosId = statisticsType.nameToId(QUEUED_TIME_NANOS);
      tasksScheduledCountId = statisticsType.nameToId(TASKS_SCHEDULED_COUNT);
      meanTaskQueuedTimeNanosId = statisticsType.nameToId(MEAN_TASK_QUEUED_TIME_NANOS);

      this.statisticsProducer = statisticsProducer;

      this.path = path;
    }

    @Override
    public void sample() {
      try {
        final Stream<String> lines = Files.lines(path);
        parse(lines);
      } catch (final IOException _ignored) {
        // this is what other statistics samplers do so I am following suit
      }
    }

    private void parse(final Stream<String> lines) {
      // Parser lambda demands final variables. Atomics work fine.
      final AtomicInteger lineNumber = new AtomicInteger(0);
      final AtomicBoolean terminated = new AtomicBoolean(false);

      lines.forEach(line -> {
        if (terminated.get()) {
          return;
        }
        switch (lineNumber.incrementAndGet()) {
          case 1: { // check format version
            final String[] tokens = tokenizeLine(line);
            if (tokens.length < 2 || !tokens[1].equals(SCHEDSTAT_FORMAT_VERSION)) {
              failParsing(terminated);
            }
          }
            break;
          case 2: // skip timestamp
            break;
          default: { // parse any other kind of line
            final String[] tokens = tokenizeLine(line);
            if (tokens.length < 9) {
              failParsing(terminated);
            }
            final String cpuId = tokens[0];
            if (!cpuId.startsWith("cpu")) {
              return; // skip non-cpu lines i.e. domainXX
            }

            final Statistics statistics = getStatisticsFor(cpuId);

            final long queuedTimeOld = statistics.getLong(queuedTimeNanosId);
            final Long queuedTimeNew = Long.valueOf(tokens[8]);
            final long tasksScheduledCountOld = statistics.getLong(tasksScheduledCountId);
            final Long tasksScheduledCountNew = Long.valueOf(tokens[9]);

            final double meanTaskQueueTime;
            final long tasksScheduledCountDelta = tasksScheduledCountNew - tasksScheduledCountOld;
            if (tasksScheduledCountDelta > 0) {
              meanTaskQueueTime =
                  ((double) queuedTimeNew - queuedTimeOld) / (tasksScheduledCountDelta);
            } else {
              meanTaskQueueTime = 0;
            }

            statistics.setLong(runningTimeNanosId, Long.valueOf(tokens[7]));
            statistics.setLong(queuedTimeNanosId, queuedTimeNew);
            statistics.setLong(tasksScheduledCountId, tasksScheduledCountNew);
            statistics.setDouble(meanTaskQueuedTimeNanosId, meanTaskQueueTime);
          }
        }
      });
    }

    /**
     * Creates {@link Statistics} for a particular CPU on-the-fly
     */
    private Statistics getStatisticsFor(final String cpuId) {
      // memoize this function so we only create statistics once per CPU
      return statisticsForCpuIds.computeIfAbsent(cpuId,
          cpuId2 -> statisticsProducer.create(statisticsType, cpuId2));
    }

    private void failParsing(final AtomicBoolean terminated) {
      // silently fail to process a file we don't understand
      terminated.set(true);
    }

    private String[] tokenizeLine(final String line) {
      return line.split("\\s+");
    }
  }

}
