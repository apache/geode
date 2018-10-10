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

import java.util.Collections;
import java.util.Map;

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
import org.apache.geode.cache.asyncqueue.internal.AsyncEventQueueImpl;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.RepositoryManager;
import org.apache.geode.cache.lucene.internal.xml.LuceneIndexCreation;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.internal.cache.RegionListener;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.logging.LogService;

public abstract class LuceneIndexImpl implements InternalLuceneIndex {
  protected static final Logger logger = LogService.getLogger();

  protected final String indexName;
  protected final String regionPath;
  protected final InternalCache cache;
  protected final LuceneIndexStats indexStats;

  protected Map<String, Analyzer> fieldAnalyzers;
  protected String[] searchableFieldNames;
  protected RepositoryManager repositoryManager;
  protected Analyzer analyzer;
  protected LuceneSerializer luceneSerializer;
  protected LocalRegion dataRegion;

  protected LuceneIndexImpl(String indexName, String regionPath, InternalCache cache) {
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

  protected LocalRegion assignDataRegion() {
    return (LocalRegion) cache.getRegion(regionPath);
  }

  protected LocalRegion getDataRegion() {
    return dataRegion;
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

  public LuceneSerializer getLuceneSerializer() {
    return this.luceneSerializer;
  }

  public void setLuceneSerializer(LuceneSerializer serializer) {
    this.luceneSerializer = serializer;
  }

  public Cache getCache() {
    return this.cache;
  }

  public void setFieldAnalyzers(Map<String, Analyzer> fieldAnalyzers) {
    this.fieldAnalyzers =
        fieldAnalyzers == null ? null : Collections.unmodifiableMap(fieldAnalyzers);
  }

  public LuceneIndexStats getIndexStats() {
    return indexStats;
  }

  public void initialize() {
    /* create index region */
    dataRegion = assignDataRegion();
    createLuceneListenersAndFileChunkRegions((PartitionedRepositoryManager) repositoryManager);
    addExtension(dataRegion);
  }

  protected void setupRepositoryManager(LuceneSerializer luceneSerializer) {
    repositoryManager = createRepositoryManager(luceneSerializer);
  }

  protected abstract RepositoryManager createRepositoryManager(LuceneSerializer luceneSerializer);

  protected abstract void createLuceneListenersAndFileChunkRegions(
      PartitionedRepositoryManager partitionedRepositoryManager);

  protected AsyncEventQueue createAEQ(Region dataRegion) {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(getName(), regionPath);
    return createAEQ(createAEQFactory(dataRegion.getAttributes()), aeqId);
  }

  protected AsyncEventQueue createAEQ(RegionAttributes attributes, String aeqId) {
    if (attributes.getPartitionAttributes() != null) {
      if (attributes.getPartitionAttributes().getLocalMaxMemory() == 0) {
        // accessor will not create AEQ
        return null;
      }
    }
    return createAEQ(createAEQFactory(attributes), aeqId);
  }

  private AsyncEventQueue createAEQ(AsyncEventQueueFactoryImpl factory, String aeqId) {
    LuceneEventListener listener = new LuceneEventListener(cache, repositoryManager);
    factory.setGatewayEventSubstitutionListener(new LuceneEventSubstitutionFilter());
    AsyncEventQueue indexQueue = factory.create(aeqId, listener);
    return indexQueue;
  }

  private AsyncEventQueueFactoryImpl createAEQFactory(final RegionAttributes attributes) {
    AsyncEventQueueFactoryImpl factory =
        (AsyncEventQueueFactoryImpl) cache.createAsyncEventQueueFactory();
    if (attributes.getPartitionAttributes() != null) {
      factory.setParallel(true); // parallel AEQ for PR
    } else {
      factory.setParallel(false); // TODO: not sure if serial AEQ working or not
    }
    factory.setMaximumQueueMemory(1000);
    factory.setDispatcherThreads(10);
    factory.setBatchSize(1000);
    factory.setIsMetaQueue(true);
    if (attributes.getDataPolicy().withPersistence()) {
      factory.setPersistent(true);
    }
    factory.setDiskStoreName(attributes.getDiskStoreName());
    factory.setDiskSynchronous(true);
    factory.setForwardExpirationDestroy(true);
    return factory;
  }

  /**
   * Register an extension with the region so that xml will be generated for this index.
   */
  protected void addExtension(LocalRegion dataRegion) {
    LuceneIndexCreation creation = new LuceneIndexCreation();
    creation.setName(this.getName());
    creation.addFieldNames(this.getFieldNames());
    creation.setRegion(dataRegion);
    creation.setFieldAnalyzers(this.getFieldAnalyzers());
    creation.setLuceneSerializer(this.getLuceneSerializer());
    dataRegion.getExtensionPoint().addExtension(creation);
  }

  public void destroy(boolean initiator) {
    // Find and delete the appropriate extension
    Extension extensionToDelete = null;
    for (Extension extension : getDataRegion().getExtensionPoint().getExtensions()) {
      LuceneIndexCreation index = (LuceneIndexCreation) extension;
      if (index.getName().equals(indexName)) {
        extensionToDelete = extension;
        break;
      }
    }
    if (extensionToDelete != null) {
      getDataRegion().getExtensionPoint().removeExtension(extensionToDelete);
    }

    // Destroy the async event queue
    destroyAsyncEventQueue(initiator);

    // Close the repository manager
    repositoryManager.close();

    RegionListener listenerToRemove = getRegionListener();
    if (listenerToRemove != null) {
      cache.removeRegionListener(listenerToRemove);
    }

    // Remove cache service profile
    dataRegion
        .removeCacheServiceProfile(LuceneIndexCreationProfile.generateId(indexName, regionPath));
  }

  private RegionListener getRegionListener() {
    RegionListener rl = null;
    for (RegionListener listener : cache.getRegionListeners()) {
      if (listener instanceof LuceneRegionListener) {
        LuceneRegionListener lrl = (LuceneRegionListener) listener;
        if (lrl.getRegionPath().equals(regionPath) && lrl.getIndexName().equals(indexName)) {
          rl = lrl;
          break;
        }
      }
    }
    return rl;
  }

  protected <K, V> Region<K, V> createRegion(final String regionName,
      final RegionAttributes<K, V> attributes) {
    // Create InternalRegionArguments to set isUsedForMetaRegion true to suppress xml generation
    // (among other things)
    InternalRegionArguments ira =
        new InternalRegionArguments().setDestroyLockFlag(true).setRecreateFlag(false)
            .setSnapshotInputStream(null).setImageTarget(null).setIsUsedForMetaRegion(true);

    // Create the region
    try {
      return this.cache.createVMRegion(regionName, attributes, ira);
    } catch (Exception e) {
      InternalGemFireError ige = new InternalGemFireError(
          "unexpected exception");
      ige.initCause(e);
      throw ige;
    }
  }

  private void destroyAsyncEventQueue(boolean initiator) {
    String aeqId = LuceneServiceImpl.getUniqueIndexName(indexName, regionPath);

    // Get the AsyncEventQueue
    AsyncEventQueueImpl aeq = (AsyncEventQueueImpl) cache.getAsyncEventQueue(aeqId);

    // Stop the AsyncEventQueue (this stops the AsyncEventQueue's underlying GatewaySender)
    // The AsyncEventQueue can be null in an accessor member
    if (aeq != null) {
      aeq.stop();
    }

    // Remove the id from the dataRegion's AsyncEventQueue ids
    // Note: The region may already have been destroyed by a remote member
    Region region = getDataRegion();
    if (!region.isDestroyed()) {
      region.getAttributesMutator().removeAsyncEventQueueId(aeqId);
    }

    // Destroy the aeq (this also removes it from the GemFireCacheImpl)
    // The AsyncEventQueue can be null in an accessor member
    if (aeq != null) {
      aeq.destroy(initiator);
    }
    if (logger.isDebugEnabled()) {
      logger.debug("Destroyed aeqId=" + aeqId);
    }
  }
}
