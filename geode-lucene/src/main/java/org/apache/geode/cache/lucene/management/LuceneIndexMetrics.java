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
package org.apache.geode.cache.lucene.management;

import java.beans.ConstructorProperties;

import org.apache.geode.cache.lucene.LuceneIndex;

public class LuceneIndexMetrics {

  private final String regionPath;

  private final String indexName;

  private final int queryExecutions;

  private final long queryExecutionTime;

  private final long queryRateAverageLatency;

  private final int queryExecutionsInProgress;

  private final long queryExecutionTotalHits;

  private final int updates;

  private final long updateTime;

  private final long updateRateAverageLatency;

  private final int updatesInProgress;

  private final int commits;

  private final long commitTime;

  private final long commitRateAverageLatency;

  private final int commitsInProgress;

  private final int documents;

  /**
   * This constructor is to be used by internal JMX framework only. A user should not try to create
   * an instance of this class.
   */
  @ConstructorProperties({"regionPath", "indexName", "queryExecutions", "queryExecutionTime",
      "queryRateAverageLatency", "queryExecutionsInProgress", "queryExecutionTotalHits", "updates",
      "updateTime", "updateRateAverageLatency", "updatesInProgress", "commits", "commitTime",
      "commitRateAverageLatency", "commitsInProgress", "documents"})
  public LuceneIndexMetrics(String regionPath, String indexName, int queryExecutions,
      long queryExecutionTime, long queryRateAverageLatency, int queryExecutionsInProgress,
      long queryExecutionTotalHits, int updates, long updateTime, long updateRateAverageLatency,
      int updatesInProgress, int commits, long commitTime, long commitRateAverageLatency,
      int commitsInProgress, int documents) {
    this.regionPath = regionPath;
    this.indexName = indexName;
    this.queryExecutions = queryExecutions;
    this.queryExecutionTime = queryExecutionTime;
    this.queryRateAverageLatency = queryRateAverageLatency;
    this.queryExecutionsInProgress = queryExecutionsInProgress;
    this.queryExecutionTotalHits = queryExecutionTotalHits;
    this.updates = updates;
    this.updateTime = updateTime;
    this.updateRateAverageLatency = updateRateAverageLatency;
    this.updatesInProgress = updatesInProgress;
    this.commits = commits;
    this.commitTime = commitTime;
    this.commitRateAverageLatency = commitRateAverageLatency;
    this.commitsInProgress = commitsInProgress;
    this.documents = documents;
  }

  /**
   * Returns the {@link String} path for the region on which the {@link LuceneIndex} is created
   *
   * @return String value of the region path on the Lucene Index is created
   */
  public String getRegionPath() {
    return regionPath;
  }

  /**
   * Returns the {@link String} name of the {@link LuceneIndex} created
   *
   * @return String value of the index name
   */
  public String getIndexName() {
    return indexName;
  }

  /**
   * Returns the number of query executions using the {@link LuceneIndex}
   *
   * @return Number of queries executed using this Lucene index
   */
  public int getQueryExecutions() {
    return queryExecutions;
  }

  /**
   * Returns the time duration for execution of queries using the {@link LuceneIndex}
   *
   * @return long value for the time in nanoseconds consumed in the execution of queries using this
   *         Lucene Index
   */
  public long getQueryExecutionTime() {
    return queryExecutionTime;
  }

  /**
   * Returns the average latency for query executions using the {@link LuceneIndex}
   *
   * @return the average latency for query executions in nanoseconds using the Lucene Index
   */
  public long getQueryRateAverageLatency() {
    return queryRateAverageLatency;
  }

  /**
   * Returns the number of query executions in progress which are using the {@link LuceneIndex}
   *
   * @return the number of query executions in progress which are using the Lucene Index
   */
  public int getQueryExecutionsInProgress() {
    return queryExecutionsInProgress;
  }

  /**
   * Returns the number of hits for the query execution using the {@link LuceneIndex}
   *
   * @return the number of hits for the query execution using the Lucene Index
   */
  public long getQueryExecutionTotalHits() {
    return queryExecutionTotalHits;
  }

  /**
   * Returns the number of update operations on the {@link LuceneIndex}
   *
   * @return the number of update operations on the Lucene Index
   */
  public int getUpdates() {
    return updates;
  }

  /**
   * Returns the time consumed for the update operations on the {@link LuceneIndex}
   *
   * @return the time consumed in nanoseconds for the update operations on the Lucene Index
   */
  public long getUpdateTime() {
    return updateTime;
  }

  /**
   * Returns the average latency for the update operations on the {@link LuceneIndex}
   *
   * @return the average latency for the update operations in nanoseconds on the Lucene Index
   */
  public long getUpdateRateAverageLatency() {
    return updateRateAverageLatency;
  }

  /**
   * Returns the number of update operations in progress for the {@link LuceneIndex}
   *
   * @return the number of update operations in progress for the Lucene Index
   */
  public int getUpdatesInProgress() {
    return updatesInProgress;
  }

  /**
   * Returns the number of commit operations executed on the {@link LuceneIndex}
   *
   * @return the number of commit operations executed on the Lucene Index
   */
  public int getCommits() {
    return commits;
  }

  /**
   * Returns the time consumed by the commit operations on the {@link LuceneIndex}
   *
   * @return the time consumed in nanoseconds by the commit operations on the Lucene Index
   */
  public long getCommitTime() {
    return commitTime;
  }

  /**
   * Returns the average latency for the commit operations using the {@link LuceneIndex}
   *
   * @return Returns the average latency for the commit operations in nanoseconds using the Lucene
   *         Index
   */
  public long getCommitRateAverageLatency() {
    return commitRateAverageLatency;
  }

  /**
   * Returns the number of commit operations in progress for the {@link LuceneIndex}
   *
   * @return Returns the number of commit operations in progress for the Lucene Indexes
   */
  public int getCommitsInProgress() {
    return commitsInProgress;
  }

  /**
   * Returns the number of documents indexed by {@link LuceneIndex}
   *
   * @return Returns the number of documents indexed by Lucene
   */
  public int getDocuments() {
    return documents;
  }

  /**
   * Outputs the string message containing all the stats stored for the {@link LuceneIndex}
   *
   * @return the string message containing all the stats stored for the Lucene Index
   */
  @Override
  public String toString() {
    return new StringBuilder().append(getClass().getSimpleName()).append("[").append("regionPath=")
        .append(regionPath).append("; indexName=").append(indexName)
        .append("; queryExecutions=").append(queryExecutions).append("; queryExecutionTime=")
        .append(queryExecutionTime).append("; queryRateAverageLatency=")
        .append(queryRateAverageLatency).append("; queryExecutionsInProgress=")
        .append(queryExecutionsInProgress).append("; queryExecutionTotalHits=")
        .append(queryExecutionTotalHits).append("; updates=").append(updates)
        .append("; updateTime=").append(updateTime).append("; updateRateAverageLatency=")
        .append(updateRateAverageLatency).append("; updatesInProgress=")
        .append(updatesInProgress).append("; commits=").append(commits)
        .append("; commitTime=").append(commitTime).append("; commitRateAverageLatency=")
        .append(commitRateAverageLatency).append("; commitsInProgress=")
        .append(commitsInProgress).append("; documents=").append(documents).append("]")
        .toString();
  }
}
