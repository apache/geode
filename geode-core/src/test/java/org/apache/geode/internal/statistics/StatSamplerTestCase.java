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
package org.apache.geode.internal.statistics;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.Statistics;
import org.apache.geode.StatisticsType;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.internal.util.StopWatch;

/**
 * Integration TestCase for StatSampler.
 *
 * @since GemFire 7.0
 */
public abstract class StatSamplerTestCase {

  protected static final Logger logger = LogService.getLogger();

  protected abstract StatisticsManager getStatisticsManager();

  protected int getStatListModCount() {
    return getStatisticsManager().getStatListModCount();
  }

  protected List<Statistics> getStatsList() {
    return getStatisticsManager().getStatsList();
  }

  protected static void waitForFileToExist(final File file, final long millis, final long sleep) {
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < millis; done =
          (file.exists())) {
        Thread.sleep(sleep);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("Waiting for file to exist: " + file, done);
  }

  protected static void waitForFileToDelete(final File file, final long millis, final long sleep) {
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < millis; done =
          (!file.exists())) {
        Thread.sleep(sleep);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("Waiting for file to be deleted: " + file, done);
  }

  protected static void waitForExpectedStatValue(final Statistics statSamplerStats,
      final String statName, final int expectedStatValue, final long millis, final long sleep) {
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < millis; done =
          (statSamplerStats.getInt(statName) >= expectedStatValue)) {
        Thread.sleep(sleep);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertTrue("Waiting for " + statName + " >= " + expectedStatValue, done);
  }

  protected static void waitForStatSample(final Statistics statSamplerStats, final int samples,
      final long millis, final long sleep) {
    waitForExpectedStatValue(statSamplerStats, "sampleCount", samples, millis, sleep);
  }

  protected static void assertStatValueDoesNotChange(final Statistics statSamplerStats,
      final String statName, final int expectedStatValue, final long millis, final long sleep) {
    boolean done = false;
    try {
      for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < millis; done =
          (statSamplerStats.getInt(statName) != expectedStatValue)) {
        Thread.sleep(sleep);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    assertFalse("Waiting to assert that " + statName + " does not change from " + expectedStatValue,
        done);
  }

  protected static class AllStatistics {

    private final HostStatSampler statSampler;
    private final Map<StatisticsType, Set<Statistics>> allStatistics;

    protected AllStatistics(HostStatSampler statSampler) throws InterruptedException {
      this.statSampler = statSampler;
      this.allStatistics = initAllStatistics();
    }

    private Map<StatisticsType, Set<Statistics>> initAllStatistics() throws InterruptedException {
      assertTrue(this.statSampler.waitForInitialization(5000));

      Map<StatisticsType, Set<Statistics>> statsTypeToStats =
          new HashMap<StatisticsType, Set<Statistics>>();
      Statistics[] stats = this.statSampler.getStatistics();
      for (int i = 0; i < stats.length; i++) {
        StatisticsType statsType = stats[i].getType();
        Set<Statistics> statsSet = statsTypeToStats.get(statsType);
        if (statsSet == null) {
          statsSet = new HashSet<Statistics>();
          statsSet.add(stats[i]);
          statsTypeToStats.put(statsType, statsSet);
        } else {
          statsSet.add(stats[i]);
        }
      }
      return statsTypeToStats;
    }

    protected boolean containsStatisticsType(StatisticsType type) {
      throw new UnsupportedOperationException("TODO");
    }

    protected boolean containsStatisticsType(String typeName) throws InterruptedException {
      for (StatisticsType statType : this.allStatistics.keySet()) {
        if (statType.getName().equals(typeName)) {
          return true;
        }
      }
      return false;
    }

    protected boolean containsStatistics(Statistics statistics) {
      throw new UnsupportedOperationException("TODO");
    }

    protected boolean containsStatistics(String instanceName) throws InterruptedException {
      for (StatisticsType statType : this.allStatistics.keySet()) {
        for (Statistics statistics : this.allStatistics.get(statType)) {
          if (statistics.getTextId().equals(instanceName)) {
            return true;
          }
        }
      }
      return false;
    }

    /**
     * Statistics[0]: typeName=StatSampler instanceName=statSampler Statistics[1]: typeName=VMStats
     * instanceName=vmStats Statistics[2]: typeName=VMMemoryUsageStats
     * instanceName=vmHeapMemoryStats Statistics[3]: typeName=VMMemoryUsageStats
     * instanceName=vmNonHeapMemoryStats Statistics[4]: typeName=VMMemoryPoolStats instanceName=Code
     * Cache-Non-heap memory Statistics[5]: typeName=VMMemoryPoolStats instanceName=PS Eden
     * Space-Heap memory Statistics[6]: typeName=VMMemoryPoolStats instanceName=PS Survivor
     * Space-Heap memory Statistics[7]: typeName=VMMemoryPoolStats instanceName=PS Old Gen-Heap
     * memory Statistics[8]: typeName=VMMemoryPoolStats instanceName=PS Perm Gen-Non-heap memory
     * Statistics[9]: typeName=VMGCStats instanceName=PS Scavenge Statistics[10]: typeName=VMGCStats
     * instanceName=PS MarkSweep Statistics[11]: typeName=LinuxSystemStats
     * instanceName=kuwait.gemstone.com Statistics[12]: typeName=LinuxProcessStats
     * instanceName=javaApp0-proc
     */
    protected void dumpStatistics() throws InterruptedException {
      Statistics[] stats = this.statSampler.getStatistics();
      for (int i = 0; i < stats.length; i++) {
        logger.info("Statistics[{}]: typeName={} instanceName={}", i, stats[i].getType().getName(),
            stats[i].getTextId());
      }
    }
  }
}
