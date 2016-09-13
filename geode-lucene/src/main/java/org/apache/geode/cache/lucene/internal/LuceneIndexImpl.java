/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.cache.lucene.internal;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.geode.InternalGemFireError;
import org.apache.geode.cache.Cache;
import org.apache.geode.cache.DataPolicy;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionAttributes;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueFactoryImpl;
import org.apache.geode.cache.lucene.internal.filesystem.ChunkKey;
import org.apache.geode.cache.lucene.internal.filesystem.File;
import org.apache.geode.cache.lucene.internal.filesystem.FileSystemStats;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.xml.LuceneIndexCreation;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.PartitionedRegion;
import org.apache.geode.internal.i18n.LocalizedStrings;
import org.apache.geode.internal.logging.LogService;

public abstract class LuceneIndexImpl implements InternalLuceneIndex {
  protected static final Logger logger = LogService.getLogger();
  
  protected final String indexName;
  protected final String regionPath;
  protected final Cache cache;
  protected final LuceneIndexStats indexStats;

  protected boolean hasInitialized = false;
  protected Map<String, Analyzer> fieldAnalyzers;
  protected String[] searchableFieldNames;
  protected RepositoryManager repositoryManager;
  protected Analyzer analyzer;
  protected LocalRegion dataRegion;

  protected LuceneIndexImpl(String indexName, String regionPath, Cache cache) {
    this.indexName = indexName;
    this.regionPath = regionPath;
    this.cache = cache;
    
    final String statsName = indexName + "-" + regionPath;
    this.indexStats = new LuceneIndexStats(cache.getDistributedSystem(), statsName);
  }

  @Override
  public String getName() {
    return this.indexName;
  }

  @Override
  public String getRegionPath() {
    return this.regionPath;
  }
 
  protected LocalRegion getDataRegion() {
    return (LocalRegion)cache.getRegion(regionPath);
  }

  protected boolean withPersistence() {
    RegionAttributes ra = dataRegion.getAttributes();
    DataPolicy dp = ra.getDataPolicy();
    final boolean withPersistence = dp.withPersistence();
    return withPersistence;
  }
  
  protected void setSearchableFields(String[] fields) {
    searchableFieldNames = fields;
  }

  @Override
  public boolean waitUntilFlushed(int maxWaitInMillisecond) {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(indexName, regionPath);
    AsyncEventQueue queue = (AsyncEventQueue)cache.getAsyncEventQueue(aeqId);
    boolean flushed = false;
    if (queue != null) {
      long start = System.nanoTime();
      while (System.nanoTime() - start < TimeUnit.MILLISECONDS.toNanos(maxWaitInMillisecond)) {
        if (0 == queue.size()) {
          flushed = true;
          break;
        } else {
          try {
            Thread.sleep(200);
          } catch (InterruptedException e) {
          }
        }
      }
    } else { 
      throw new IllegalArgumentException("The AEQ does not exist for the index "+indexName+" region "+regionPath);
    }

    return flushed;
  }

  @Override
  public String[] getFieldNames() {
    return searchableFieldNames;
  }

  @Override
  public Map<String, Analyzer> getFieldAnalyzers() {
    return this.fieldAnalyzers;
  }

  public RepositoryManager getRepositoryManager() {
    return this.repositoryManager;
  }
  
  public void setAnalyzer(Analyzer analyzer) {
    if (analyzer == null) {
      this.analyzer = new StandardAnalyzer();
    } else {
      this.analyzer = analyzer;
    }
  }

  public Analyzer getAnalyzer() {
    return this.analyzer;
  }

  public Cache getCache() {
    return this.cache;
  }
  
  public void setFieldAnalyzers(Map<String, Analyzer> fieldAnalyzers) {
    this.fieldAnalyzers = fieldAnalyzers == null ? null : Collections.unmodifiableMap(fieldAnalyzers);
  }

  public LuceneIndexStats getIndexStats() {
    return indexStats;
  }

  protected void initialize() {
    if (!hasInitialized) {
      /* create index region */
      dataRegion = getDataRegion();
      //assert dataRegion != null;

      repositoryManager = createRepositoryManager();
      
      // create AEQ, AEQ listener and specify the listener to repositoryManager
      createAEQ(dataRegion);

      addExtension(dataRegion);
      hasInitialized = true;
    }
  }
  
  protected abstract RepositoryManager createRepositoryManager();
  
  protected AsyncEventQueue createAEQ(Region dataRegion) {
    return createAEQ(createAEQFactory(dataRegion));
  }

  private AsyncEventQueueFactoryImpl createAEQFactory(final Region dataRegion) {
    AsyncEventQueueFactoryImpl factory = (AsyncEventQueueFactoryImpl) cache.createAsyncEventQueueFactory();
    if (dataRegion instanceof PartitionedRegion) {
      factory.setParallel(true); // parallel AEQ for PR
    } else {
      factory.setParallel(false); // TODO: not sure if serial AEQ working or not
    }
    factory.setMaximumQueueMemory(1000);
    factory.setDispatcherThreads(10);
    factory.setIsMetaQueue(true);
    if (dataRegion.getAttributes().getDataPolicy().withPersistence()) {
      factory.setPersistent(true);
    }
    factory.setDiskStoreName(dataRegion.getAttributes().getDiskStoreName());
    factory.setDiskSynchronous(dataRegion.getAttributes().isDiskSynchronous());
    factory.setForwardExpirationDestroy(true);
    return factory;
  }

  private AsyncEventQueue createAEQ(AsyncEventQueueFactoryImpl factory) {
    LuceneEventListener listener = new LuceneEventListener(repositoryManager);
    String aeqId = LuceneServiceImpl.getUniqueIndexName(getName(), regionPath);
    AsyncEventQueue indexQueue = factory.create(aeqId, listener);
    return indexQueue;
  }

/**
   * Register an extension with the region
   * so that xml will be generated for this index.
   */
  protected void addExtension(LocalRegion dataRegion) {
    LuceneIndexCreation creation = new LuceneIndexCreation();
    creation.setName(this.getName());
    creation.addFieldNames(this.getFieldNames());
    creation.setRegion(dataRegion);
    creation.setFieldAnalyzers(this.getFieldAnalyzers());
    dataRegion.getExtensionPoint().addExtension(creation);
  }

  protected <K, V> Region<K, V> createRegion(final String regionName, final RegionAttributes<K, V> attributes) {
    // Create InternalRegionArguments to set isUsedForMetaRegion true to suppress xml generation (among other things)
    InternalRegionArguments ira = new InternalRegionArguments().setDestroyLockFlag(true).setRecreateFlag(false)
        .setSnapshotInputStream(null).setImageTarget(null).setIsUsedForMetaRegion(true);

    // Create the region
    try {
      return ((GemFireCacheImpl)this.cache).createVMRegion(regionName, attributes, ira);
    } catch (Exception e) {
      InternalGemFireError ige = new InternalGemFireError(LocalizedStrings.GemFireCache_UNEXPECTED_EXCEPTION.toLocalizedString());
      ige.initCause(e);
      throw ige;
    }
  }
}
