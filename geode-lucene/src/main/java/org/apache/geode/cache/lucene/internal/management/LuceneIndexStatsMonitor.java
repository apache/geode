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
package org.apache.geode.cache.lucene.internal.management;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.management.internal.ManagementStrings;
import org.apache.geode.management.internal.beans.stats.MBeanStatsMonitor;
import org.apache.geode.management.internal.beans.stats.StatType;
import org.apache.geode.management.internal.beans.stats.StatsAverageLatency;
import org.apache.geode.management.internal.beans.stats.StatsRate;

public class LuceneIndexStatsMonitor extends MBeanStatsMonitor {

  private StatsRate updateRate;

  private StatsAverageLatency updateRateAverageLatency;

  private StatsRate commitRate;

  private StatsAverageLatency commitRateAverageLatency;

  private StatsRate queryRate;

  private StatsAverageLatency queryRateAverageLatency;

  public static final String LUCENE_SERVICE_MXBEAN_MONITOR_PREFIX = "LuceneServiceMXBeanMonitor_";

  public LuceneIndexStatsMonitor(LuceneIndex index) {
    super(LUCENE_SERVICE_MXBEAN_MONITOR_PREFIX + index.getRegionPath() + "_" + index.getName());
    addStatisticsToMonitor(((LuceneIndexImpl) index).getIndexStats().getStats());
    configureMetrics();
  }

  private void configureMetrics() {
    this.queryRate = new StatsRate(StatsKey.QUERIES, StatType.INT_TYPE, this);

    this.updateRate = new StatsRate(StatsKey.UPDATES, StatType.INT_TYPE, this);

    this.commitRate = new StatsRate(StatsKey.COMMITS, StatType.INT_TYPE, this);

    this.queryRateAverageLatency =
        new StatsAverageLatency(StatsKey.QUERIES, StatType.INT_TYPE, StatsKey.QUERY_TIME, this);

    this.updateRateAverageLatency =
        new StatsAverageLatency(StatsKey.UPDATES, StatType.INT_TYPE, StatsKey.UPDATE_TIME, this);

    this.commitRateAverageLatency =
        new StatsAverageLatency(StatsKey.COMMITS, StatType.INT_TYPE, StatsKey.COMMIT_TIME, this);
  }

  protected LuceneIndexMetrics getIndexMetrics(LuceneIndex index) {
    int queryExecutions = getStatistic(StatsKey.QUERIES).intValue();
    long queryExecutionTime = getStatistic(StatsKey.QUERY_TIME).longValue();
    float queryRateValue = this.queryRate.getRate();
    long queryRateAverageLatencyValue = this.queryRateAverageLatency.getAverageLatency();
    int queryExecutionsInProgress = getStatistic(StatsKey.QUERIES_IN_PROGRESS).intValue();
    long queryExecutionTotalHits = getStatistic(StatsKey.QUERIES_TOTAL_HITS).longValue();

    int updates = getStatistic(StatsKey.UPDATES).intValue();
    long updateTime = getStatistic(StatsKey.UPDATE_TIME).longValue();
    float updateRateValue = this.updateRate.getRate();
    long updateRateAverageLatencyValue = this.updateRateAverageLatency.getAverageLatency();
    int updatesInProgress = getStatistic(StatsKey.UPDATES_IN_PROGRESS).intValue();

    int commits = getStatistic(StatsKey.COMMITS).intValue();
    long commitTime = getStatistic(StatsKey.COMMIT_TIME).longValue();
    float commitRateValue = this.commitRate.getRate();
    long commitRateAverageLatencyValue = this.commitRateAverageLatency.getAverageLatency();
    int commitsInProgress = getStatistic(StatsKey.COMMITS_IN_PROGRESS).intValue();

    int documents = getStatistic(StatsKey.DOCUMENTS).intValue();

    return new LuceneIndexMetrics(index.getRegionPath(), index.getName(), queryExecutions,
        queryExecutionTime, queryRateValue, queryRateAverageLatencyValue, queryExecutionsInProgress,
        queryExecutionTotalHits, updates, updateTime, updateRateValue,
        updateRateAverageLatencyValue, updatesInProgress, commits, commitTime, commitRateValue,
        commitRateAverageLatencyValue, commitsInProgress, documents);
  }
}
