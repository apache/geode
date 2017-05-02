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

import java.util.Map;

import org.apache.lucene.analysis.Analyzer;

import org.apache.geode.cache.AttributesFactory;
import org.apache.geode.cache.EvictionAlgorithm;
import org.apache.geode.cache.EvictionAttributes;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.RegionListener;

public class LuceneRegionListener implements RegionListener {

  private final LuceneServiceImpl service;

  private final InternalCache cache;

  private final String indexName;

  private final String regionPath;

  private final Analyzer analyzer;

  private final Map<String, Analyzer> fieldAnalyzers;

  private final String[] fields;

  private LuceneIndexImpl luceneIndex;

  public LuceneRegionListener(LuceneServiceImpl service, InternalCache cache, String indexName,
      String regionPath, String[] fields, Analyzer analyzer, Map<String, Analyzer> fieldAnalyzers) {
    this.service = service;
    this.cache = cache;
    this.indexName = indexName;
    this.regionPath = regionPath;
    this.fields = fields;
    this.analyzer = analyzer;
    this.fieldAnalyzers = fieldAnalyzers;
  }

  public String getRegionPath() {
    return this.regionPath;
  }

  public String getIndexName() {
    return this.indexName;
  }

  @Override
  public RegionAttributes beforeCreate(Region parent, String regionName, RegionAttributes attrs,
      InternalRegionArguments internalRegionArgs) {
    RegionAttributes updatedRA = attrs;
    String path = parent == null ? "/" + regionName : parent.getFullPath() + "/" + regionName;

    if (path.equals(this.regionPath)) {

      if (!attrs.getDataPolicy().withPartitioning()) {
        // replicated region
        throw new UnsupportedOperationException(
            "Lucene indexes on replicated regions are not supported");
      }

      // For now we cannot support eviction with local destroy.
      // Eviction with overflow to disk still needs to be supported
      EvictionAttributes evictionAttributes = attrs.getEvictionAttributes();
      EvictionAlgorithm evictionAlgorithm = evictionAttributes.getAlgorithm();
      if (evictionAlgorithm != EvictionAlgorithm.NONE
          && evictionAttributes.getAction().isLocalDestroy()) {
        throw new UnsupportedOperationException(
            "Lucene indexes on regions with eviction and action local destroy are not supported");
      }

      String aeqId = LuceneServiceImpl.getUniqueIndexName(this.indexName, this.regionPath);
      if (!attrs.getAsyncEventQueueIds().contains(aeqId)) {
        AttributesFactory af = new AttributesFactory(attrs);
        af.addAsyncEventQueueId(aeqId);
        updatedRA = af.create();
      }

      // Add index creation profile
      internalRegionArgs.addCacheServiceProfile(new LuceneIndexCreationProfile(this.indexName,
          this.regionPath, this.fields, this.analyzer, this.fieldAnalyzers));

      luceneIndex = this.service.beforeDataRegionCreated(this.indexName, this.regionPath, attrs,
          this.analyzer, this.fieldAnalyzers, aeqId, this.fields);

      // Add internal async event id
      internalRegionArgs.addInternalAsyncEventQueueId(aeqId);
    }
    return updatedRA;
  }

  @Override
  public void afterCreate(Region region) {
    if (region.getFullPath().equals(this.regionPath)) {
      this.service.afterDataRegionCreated(this.luceneIndex);
      String aeqId = LuceneServiceImpl.getUniqueIndexName(this.indexName, this.regionPath);
      AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);
      AbstractPartitionedRepositoryManager repositoryManager =
          (AbstractPartitionedRepositoryManager) luceneIndex.getRepositoryManager();
      repositoryManager.allowRepositoryComputation();
      this.cache.removeRegionListener(this);
    }
  }
}
