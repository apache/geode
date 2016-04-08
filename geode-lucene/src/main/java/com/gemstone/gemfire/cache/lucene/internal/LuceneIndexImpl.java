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

import java.util.Map;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.ChunkKey;
import com.gemstone.gemfire.cache.lucene.internal.filesystem.File;
import com.gemstone.gemfire.cache.lucene.internal.repository.RepositoryManager;
import com.gemstone.gemfire.cache.lucene.internal.xml.LuceneIndexCreation;
import com.gemstone.gemfire.internal.cache.PartitionedRegion;
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
  public String[] getFieldNames() {
    return searchableFieldNames;
  }

  @Override
  public Map<String, Analyzer> getFieldAnalyzerMap() {
    // TODO Auto-generated method stub
    // Will do that later: Gester
    return null;
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
    creation.setFieldFieldAnalyzerMap(this.getFieldAnalyzerMap());
    dataRegion.getExtensionPoint().addExtension(creation);
  }
}
