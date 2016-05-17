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

package com.gemstone.gemfire.cache.lucene.internal;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.gemstone.gemfire.InternalGemFireError;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionAttributes;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventQueue;
import com.gemstone.gemfire.cache.asyncqueue.internal.AsyncEventQueueImpl;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.xml.LuceneIndexCreation;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.internal.cache.InternalRegionArguments;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
import com.gemstone.gemfire.internal.i18n.LocalizedStrings;
import com.gemstone.gemfire.internal.logging.LogService;

public abstract class LuceneIndexImpl implements InternalLuceneIndex {
  protected static final Logger logger = LogService.getLogger();
  
//  protected HashSet<String> searchableFieldNames = new HashSet<String>();
  String[] searchableFieldNames;
  protected RepositoryManager repositoryManager;
  protected Analyzer analyzer;
  
  Region<String, File> fileRegion;
  Region<ChunkKey, byte[]> chunkRegion;
  
  protected String indexName;
  protected String regionPath;
  protected boolean hasInitialized = false;
  protected Map<String, Analyzer> fieldAnalyzers;

  protected final Cache cache;
  
  protected LuceneIndexImpl(String indexName, String regionPath, Cache cache) {
    this.indexName = indexName;
    this.regionPath = regionPath;
    this.cache = cache;
  }

  @Override
  public String getName() {
    return this.indexName;
  }

  @Override
  public String getRegionPath() {
    return this.regionPath;
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
          logger.debug("waitUntilFlushed: Queue size is 0");
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

  public void setFieldAnalyzers(Map<String, Analyzer> fieldAnalyzers) {
    this.fieldAnalyzers = fieldAnalyzers == null ? null : Collections.unmodifiableMap(fieldAnalyzers);
  }

  protected abstract void initialize();
  
  /**
   * Register an extension with the region
   * so that xml will be generated for this index.
   */
  protected void addExtension(PartitionedRegion dataRegion) {
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
