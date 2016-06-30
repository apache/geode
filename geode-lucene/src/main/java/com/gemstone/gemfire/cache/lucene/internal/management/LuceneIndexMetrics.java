/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal.management;

import java.beans.ConstructorProperties;

public class LuceneIndexMetrics {

  private final String regionPath;

  private final String indexName;

  private final int queryExecutions;

  private final long queryExecutionTime;

  private final int queryExecutionsInProgress;

  private final long queryExecutionTotalHits;

  private final int updates;

  private final long updateTime;

  private final int updatesInProgress;

  private final int commits;

  private final long commitTime;

  private final int commitsInProgress;

  private final int documents;

  /**
   * This constructor is to be used by internal JMX framework only. A user should
   * not try to create an instance of this class.
   */
  @ConstructorProperties( { "regionPath", "indexName", "queryExecutions", "queryExecutionTime",
      "queryExecutionsInProgress", "queryExecutionTotalHits", "updates", "updateTime", "updatesInProgress",
      "commits", "commitTime", "commitsInProgress", "documents"
  })
  public LuceneIndexMetrics(String regionPath, String indexName, int queryExecutions, long queryExecutionTime,
      int queryExecutionsInProgress, long queryExecutionTotalHits, int updates, long updateTime,
      int updatesInProgress, int commits, long commitTime, int commitsInProgress, int documents) {
    this.regionPath = regionPath;
    this.indexName = indexName;
    this.queryExecutions = queryExecutions;
    this.queryExecutionTime = queryExecutionTime;
    this.queryExecutionsInProgress = queryExecutionsInProgress;
    this.queryExecutionTotalHits = queryExecutionTotalHits;
    this.updates = updates;
    this.updateTime = updateTime;
    this.updatesInProgress = updatesInProgress;
    this.commits = commits;
    this.commitTime = commitTime;
    this.commitsInProgress = commitsInProgress;
    this.documents = documents;
  }

  public String getRegionPath() {
    return this.regionPath;
  }

  public String getIndexName() {
    return this.indexName;
  }

  public int getQueryExecutions() {
    return this.queryExecutions;
  }

  public long getQueryExecutionTime() {
    return this.queryExecutionTime;
  }

  public int getQueryExecutionsInProgress() {
    return this.queryExecutionsInProgress;
  }

  public long getQueryExecutionTotalHits() {
    return this.queryExecutionTotalHits;
  }

  public int getUpdates() {
    return this.updates;
  }

  public long getUpdateTime() {
    return this.updateTime;
  }

  public int getUpdatesInProgress() {
    return this.updatesInProgress;
  }

  public int getCommits() {
    return this.commits;
  }

  public long getCommitTime() {
    return this.commitTime;
  }

  public int getCommitsInProgress() {
    return this.commitsInProgress;
  }

  public int getDocuments() {
    return documents;
  }

  @Override
  public String toString() {
    return new StringBuilder()
        .append(getClass().getSimpleName())
        .append("[")
        .append("regionPath=")
        .append(this.regionPath)
        .append("; indexName=")
        .append(this.indexName)
        .append("; queryExecutions=")
        .append(this.queryExecutions)
        .append("; queryExecutionTime=")
        .append(this.queryExecutionTime)
        .append("; queryExecutionsInProgress=")
        .append(this.queryExecutionsInProgress)
        .append("; queryExecutionTotalHits=")
        .append(this.queryExecutionTotalHits)
        .append("; updates=")
        .append(this.updates)
        .append("; updateTime=")
        .append(this.updateTime)
        .append("; updatesInProgress=")
        .append(this.updatesInProgress)
        .append("; commits=")
        .append(this.commits)
        .append("; commitTime=")
        .append(this.commitTime)
        .append("; commitsInProgress=")
        .append(this.commitsInProgress)
        .append("; documents=")
        .append(this.documents)
        .append("]")
        .toString();
  }
}
