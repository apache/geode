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

import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.internal.LuceneIndexImpl;
import org.apache.geode.cache.lucene.internal.LuceneIndexStats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class LuceneServiceBridge {

  private LuceneService service;

  private Map<String, LuceneIndexStatsMonitor> monitors;

  public LuceneServiceBridge(LuceneService service) {
    this.service = service;
    this.monitors = new ConcurrentHashMap<>();
  }

  public void addIndex(LuceneIndex index) {
    // Create monitor on the index
    LuceneIndexStatsMonitor monitor = new LuceneIndexStatsMonitor(index);

    // Register the monitor
    this.monitors.put(getMonitorKey(index), monitor);
  }

  public LuceneIndexMetrics[] listIndexMetrics() {
    Collection<LuceneIndex> indexes = this.service.getAllIndexes();
    LuceneIndexMetrics[] indexMetrics = new LuceneIndexMetrics[indexes.size()];
    int i = 0;
    for (LuceneIndex index : this.service.getAllIndexes()) {
      indexMetrics[i++] = getIndexMetrics((LuceneIndexImpl) index);
    }
    return indexMetrics;
  }

  public LuceneIndexMetrics[] listIndexMetrics(String regionPath) {
    if (!regionPath.startsWith(Region.SEPARATOR)) {
      regionPath = Region.SEPARATOR + regionPath;
    }
    List<LuceneIndexMetrics> indexMetrics = new ArrayList();
    for (LuceneIndex index : this.service.getAllIndexes()) {
      if (index.getRegionPath().equals(regionPath)) {
        indexMetrics.add(getIndexMetrics((LuceneIndexImpl) index));
      }
    }
    return indexMetrics.toArray(new LuceneIndexMetrics[indexMetrics.size()]);
  }

  public LuceneIndexMetrics listIndexMetrics(String regionPath, String indexName) {
    LuceneIndexImpl index = (LuceneIndexImpl) this.service.getIndex(indexName, regionPath);
    return index == null ? null : getIndexMetrics(index);
  }

  private String getMonitorKey(LuceneIndex index) {
    return index.getRegionPath() + "_" + index.getName();
  }

  private LuceneIndexMetrics getIndexMetrics(LuceneIndexImpl index) {
    LuceneIndexStatsMonitor monitor = this.monitors.get(getMonitorKey(index));
    return monitor.getIndexMetrics(index);
  }
}
