/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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
package org.apache.geode.cache.lucene.internal;

import static org.apache.geode.distributed.internal.DistributionStats.getStatTime;

import java.util.function.IntSupplier;

import org.apache.geode.StatisticDescriptor;
import org.apache.geode.Statistics;
import org.apache.geode.StatisticsFactory;
import org.apache.geode.StatisticsType;
import org.apache.geode.StatisticsTypeFactory;
import org.apache.geode.internal.CopyOnWriteHashSet;
import org.apache.geode.internal.statistics.StatisticsTypeFactoryImpl;

public class LuceneIndexStats {
  // statistics type
  private static final StatisticsType statsType;
  private static final String statsTypeName = "LuceneIndexStats";
  private static final String statsTypeDescription = "Statistics about lucene indexes";

  private static final int queryExecutionsId;
  private static final int queryExecutionTimeId;
  private static final int queryExecutionsInProgressId;
  private static final int queryExecutionTotalHitsId;
  private static final int repositoryQueryExecutionsId;
  private static final int repositoryQueryExecutionTimeId;
  private static final int repositoryQueryExecutionsInProgressId;
  private static final int repositoryQueryExecutionTotalHitsId;
  private static final int updatesId;
  private static final int updateTimeId;
  private static final int updatesInProgressId;
  private static final int commitsId;
  private static final int commitTimeId;
  private static final int commitsInProgressId;
  private static final int documentsId;
  private static final int failedEntriesId;

  private final Statistics stats;
  private final CopyOnWriteHashSet<IntSupplier> documentsSuppliers = new CopyOnWriteHashSet<>();

  static {
    final StatisticsTypeFactory f = StatisticsTypeFactoryImpl.singleton();
    statsType = f.createType(statsTypeName, statsTypeDescription, new StatisticDescriptor[] {
        f.createIntCounter("queryExecutions", "Number of lucene queries executed on this member",
            "operations"),
        f.createLongCounter("queryExecutionTime", "Amount of time spent executing lucene queries",
            "nanoseconds"),
        f.createIntGauge("queryExecutionsInProgress",
            "Number of query executions currently in progress", "operations"),
        f.createLongCounter("queryExecutionTotalHits",
            "Total number of documents returned by query executions", "entries"),
        f.createIntCounter("repositoryQueryExecutions",
            "Number of lucene repository queries executed on this member", "operations"),
        f.createLongCounter("repositoryQueryExecutionTime",
            "Amount of time spent executing lucene repository queries", "nanoseconds"),
        f.createIntGauge("repositoryQueryExecutionsInProgress",
            "Number of repository query executions currently in progress", "operations"),
        f.createLongCounter("repositoryQueryExecutionTotalHits",
            "Total number of documents returned by repository query executions", "entries"),
        f.createIntCounter("updates",
            "Number of lucene index documents added/removed on this member", "operations"),
        f.createLongCounter("updateTime",
            "Amount of time spent adding or removing documents from the index", "nanoseconds"),
        f.createIntGauge("updatesInProgress", "Number of index updates in progress", "operations"),
        f.createIntCounter("failedEntries", "Number of entries failed to index", "entries"),
        f.createIntCounter("commits", "Number of lucene index commits on this member",
            "operations"),
        f.createLongCounter("commitTime", "Amount of time spent in lucene index commits",
            "nanoseconds"),
        f.createIntGauge("commitsInProgress", "Number of lucene index commits in progress",
            "operations"),
        f.createIntGauge("documents", "Number of documents in the index", "documents"),});

    queryExecutionsId = statsType.nameToId("queryExecutions");
    queryExecutionTimeId = statsType.nameToId("queryExecutionTime");
    queryExecutionsInProgressId = statsType.nameToId("queryExecutionsInProgress");
    queryExecutionTotalHitsId = statsType.nameToId("queryExecutionTotalHits");
    repositoryQueryExecutionsId = statsType.nameToId("repositoryQueryExecutions");
    repositoryQueryExecutionTimeId = statsType.nameToId("repositoryQueryExecutionTime");
    repositoryQueryExecutionsInProgressId =
        statsType.nameToId("repositoryQueryExecutionsInProgress");
    repositoryQueryExecutionTotalHitsId = statsType.nameToId("repositoryQueryExecutionTotalHits");
    updatesId = statsType.nameToId("updates");
    updateTimeId = statsType.nameToId("updateTime");
    updatesInProgressId = statsType.nameToId("updatesInProgress");
    commitsId = statsType.nameToId("commits");
    commitTimeId = statsType.nameToId("commitTime");
    commitsInProgressId = statsType.nameToId("commitsInProgress");
    documentsId = statsType.nameToId("documents");
    failedEntriesId = statsType.nameToId("failedEntries");
  }

  public LuceneIndexStats(StatisticsFactory f, String name) {
    this.stats = f.createAtomicStatistics(statsType, name);
    stats.setIntSupplier(documentsId, this::computeDocumentCount);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startRepositoryQuery() {
    stats.incInt(repositoryQueryExecutionsInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endRepositoryQuery(long start, final int totalHits) {
    stats.incLong(repositoryQueryExecutionTimeId, getStatTime() - start);
    stats.incInt(repositoryQueryExecutionsInProgressId, -1);
    stats.incInt(repositoryQueryExecutionsId, 1);
    stats.incLong(repositoryQueryExecutionTotalHitsId, totalHits);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startQuery() {
    stats.incInt(queryExecutionsInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endQuery(long start, final int totalHits) {
    stats.incLong(queryExecutionTimeId, getStatTime() - start);
    stats.incInt(queryExecutionsInProgressId, -1);
    stats.incLong(queryExecutionTotalHitsId, totalHits);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startUpdate() {
    stats.incInt(updatesInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endUpdate(long start) {
    stats.incLong(updateTimeId, getStatTime() - start);
    stats.incInt(updatesInProgressId, -1);
    stats.incInt(updatesId, 1);
  }

  /**
   * @return the timestamp that marks the start of the operation
   */
  public long startCommit() {
    stats.incInt(commitsInProgressId, 1);
    return getStatTime();
  }

  /**
   * @param start the timestamp taken when the operation started
   */
  public void endCommit(long start) {
    stats.incLong(commitTimeId, getStatTime() - start);
    stats.incInt(commitsInProgressId, -1);
    stats.incInt(commitsId, 1);
  }

  public void incFailedEntries() {
    stats.incInt(failedEntriesId, 1);
  }

  public int getFailedEntries() {
    return stats.getInt(failedEntriesId);
  }

  public void addDocumentsSupplier(IntSupplier supplier) {
    this.documentsSuppliers.add(supplier);
  }

  public void removeDocumentsSupplier(IntSupplier supplier) {
    this.documentsSuppliers.remove(supplier);
  }

  public int getDocuments() {
    return this.stats.getInt(documentsId);
  }

  private int computeDocumentCount() {
    return this.documentsSuppliers.stream().mapToInt(IntSupplier::getAsInt).sum();
  }

  public int getQueryExecutions() {
    return stats.getInt(queryExecutionsId);
  }

  public long getQueryExecutionTime() {
    return stats.getLong(queryExecutionTimeId);
  }

  public int getQueryExecutionsInProgress() {
    return stats.getInt(queryExecutionsInProgressId);
  }

  public long getQueryExecutionTotalHits() {
    return stats.getLong(queryExecutionTotalHitsId);
  }

  public int getUpdates() {
    return stats.getInt(updatesId);
  }

  public long getUpdateTime() {
    return stats.getLong(updateTimeId);
  }

  public int getUpdatesInProgress() {
    return stats.getInt(updatesInProgressId);
  }

  public int getCommits() {
    return stats.getInt(commitsId);
  }

  public long getCommitTime() {
    return stats.getLong(commitTimeId);
  }

  public int getCommitsInProgress() {
    return stats.getInt(commitsInProgressId);
  }

  public Statistics getStats() {
    return this.stats;
  }

  public void incNumberOfQueryExecuted() {
    stats.incInt(queryExecutionsId, 1);
  }
}
