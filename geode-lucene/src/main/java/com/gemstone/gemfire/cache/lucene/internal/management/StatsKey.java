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

public class StatsKey {

  public static final String UPDATES = "updates";
  public static final String UPDATE_TIME = "updateTime";
  public static final String UPDATES_IN_PROGRESS = "updatesInProgress";

  public static final String COMMITS = "commits";
  public static final String COMMIT_TIME = "commitTime";
  public static final String COMMITS_IN_PROGRESS = "commitsInProgress";

  public static final String QUERIES = "queryExecutions";
  public static final String QUERY_TIME = "queryExecutionTime";
  public static final String QUERIES_IN_PROGRESS = "queryExecutionsInProgress";
  public static final String QUERIES_TOTAL_HITS = "queryExecutionTotalHits";

  public static final String DOCUMENTS = "documents";
}
