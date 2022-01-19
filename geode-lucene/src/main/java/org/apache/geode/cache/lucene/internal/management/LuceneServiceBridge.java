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

import static org.apache.geode.cache.Region.SEPARATOR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.management.LuceneIndexMetrics;

public class LuceneServiceBridge {

  private final LuceneService service;

  private final Map<String, LuceneIndexStatsMonitor> monitors;

  public LuceneServiceBridge(LuceneService service) {
    this.service = service;
    monitors = new ConcurrentHashMap<>();
  }

  public void addIndex(LuceneIndex index) {
    // Create monitor on the index
    LuceneIndexStatsMonitor monitor = new LuceneIndexStatsMonitor(index);

    // Register the monitor
    monitors.put(getMonitorKey(index), monitor);
  }

  public LuceneIndexMetrics[] listIndexMetrics() {
    Collection<LuceneIndex> indexes = service.getAllIndexes();
    LuceneIndexMetrics[] indexMetrics = new LuceneIndexMetrics[indexes.size()];
    int i = 0;
    for (LuceneIndex index : service.getAllIndexes()) {
      indexMetrics[i++] = getIndexMetrics(index);
    }
    return indexMetrics;
  }

  public LuceneIndexMetrics[] listIndexMetrics(String regionPath) {
    if (!regionPath.startsWith(SEPARATOR)) {
      regionPath = SEPARATOR + regionPath;
    }
    List<LuceneIndexMetrics> indexMetrics = new ArrayList();
    for (LuceneIndex index : service.getAllIndexes()) {
      if (index.getRegionPath().equals(regionPath)) {
        indexMetrics.add(getIndexMetrics(index));
      }
    }
    return indexMetrics.toArray(new LuceneIndexMetrics[indexMetrics.size()]);
  }

  public LuceneIndexMetrics listIndexMetrics(String regionPath, String indexName) {
    LuceneIndex index = service.getIndex(indexName, regionPath);
    return index == null ? null : getIndexMetrics(index);
  }

  private String getMonitorKey(LuceneIndex index) {
    return index.getRegionPath() + "_" + index.getName();
  }

  private LuceneIndexMetrics getIndexMetrics(LuceneIndex index) {
    LuceneIndexStatsMonitor monitor = monitors.get(getMonitorKey(index));
    return monitor.getIndexMetrics(index);
  }
}
