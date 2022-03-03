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

import static org.apache.geode.cache.Region.SEPARATOR;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.lucene.LuceneIndexDestroyedException;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.cache.RegionListener;
import org.apache.geode.internal.cache.xmlcache.RegionAttributesCreation;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class LuceneRegionListener implements RegionListener {

  private final LuceneServiceImpl service;

  private final String indexName;

  private final String regionPath;

  private final Analyzer analyzer;

  private final Map<String, Analyzer> fieldAnalyzers;

  private final String[] fields;

  private final LuceneSerializer serializer;

  private InternalLuceneIndex luceneIndex;

  private final AtomicBoolean beforeCreateInvoked = new AtomicBoolean();

  private final AtomicBoolean afterCreateInvoked = new AtomicBoolean();

  private static final Logger logger = LogService.getLogger();

  public LuceneRegionListener(LuceneServiceImpl service, String indexName,
      String regionPath, String[] fields, Analyzer analyzer, Map<String, Analyzer> fieldAnalyzers,
      LuceneSerializer serializer) {
    this.service = service;
    this.indexName = indexName;
    this.regionPath = regionPath;
    this.fields = fields;
    this.analyzer = analyzer;
    this.fieldAnalyzers = fieldAnalyzers;
    this.serializer = serializer;
  }

  public String getRegionPath() {
    return regionPath;
  }

  public String getIndexName() {
    return indexName;
  }

  @Override
  public RegionAttributes beforeCreate(Region parent, String regionName, RegionAttributes attrs,
      InternalRegionArguments internalRegionArgs) {
    RegionAttributes updatedRA = attrs;
    String path =
        parent == null ? SEPARATOR + regionName : parent.getFullPath() + SEPARATOR + regionName;

    if (path.equals(regionPath) && beforeCreateInvoked.compareAndSet(false, true)) {

      LuceneServiceImpl.validateRegionAttributes(attrs);

      String aeqId = LuceneServiceImpl.getUniqueIndexName(indexName, regionPath);
      if (!attrs.getAsyncEventQueueIds().contains(aeqId)) {
        RegionAttributesCreation regionAttributesCreation =
            new RegionAttributesCreation(attrs, false);
        regionAttributesCreation.addAsyncEventQueueId(aeqId);
        updatedRA = regionAttributesCreation;
      }

      // Add index creation profile
      internalRegionArgs.addCacheServiceProfile(new LuceneIndexCreationProfile(indexName,
          regionPath, fields, analyzer, fieldAnalyzers, serializer));

      luceneIndex = service.beforeDataRegionCreated(indexName, regionPath, attrs,
          analyzer, fieldAnalyzers, aeqId, serializer, fields);

      // Add internal async event id
      internalRegionArgs.addInternalAsyncEventQueueId(aeqId);
    }
    return updatedRA;
  }

  @Override
  public void afterCreate(Region region) {
    if (region.getFullPath().equals(regionPath)
        && afterCreateInvoked.compareAndSet(false, true)) {
      try {
        service.afterDataRegionCreated(luceneIndex);
      } catch (LuceneIndexDestroyedException e) {
        logger.warn(String.format("Lucene index %s on region %s was destroyed while being created",
            indexName, regionPath));
        return;
      }
      service.createLuceneIndexOnDataRegion((PartitionedRegion) region, luceneIndex);
    }
  }

  @Override
  public void beforeDestroyed(Region region) {
    if (region.getFullPath().equals(regionPath)) {
      service.beforeRegionDestroyed(region);
    }
  }

  @Override
  public void cleanupFailedInitialization(Region region) {
    // Reset the booleans
    beforeCreateInvoked.set(false);
    afterCreateInvoked.set(false);

    // Clean up the region in the LuceneService
    if (region.getFullPath().equals(regionPath)) {
      service.cleanupFailedInitialization(region);
    }
  }
}
