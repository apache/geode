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

package com.gemstone.gemfire.cache.lucene.internal.xml;

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.lucene.LuceneIndex;
import com.gemstone.gemfire.cache.lucene.LuceneService;
import com.gemstone.gemfire.cache.lucene.LuceneServiceProvider;
import com.gemstone.gemfire.cache.lucene.internal.LuceneServiceImpl;
import com.gemstone.gemfire.internal.cache.extension.Extensible;
import com.gemstone.gemfire.internal.cache.extension.Extension;
import com.gemstone.gemfire.internal.cache.xmlcache.XmlGenerator;

public class LuceneIndexCreation implements LuceneIndex, Extension<Region<?, ?>> {
  private Region region;
  private String name;
  private Set<String> fieldNames = new LinkedHashSet<String>();
  private Map<String, Analyzer> fieldFieldAnalyzerMap;

  
  public void setRegion(Region region) {
    this.region = region;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Map<String, Analyzer> getFieldFieldAnalyzerMap() {
    return fieldFieldAnalyzerMap;
  }

  public void setFieldFieldAnalyzerMap(
      Map<String, Analyzer> fieldFieldAnalyzerMap) {
    this.fieldFieldAnalyzerMap = fieldFieldAnalyzerMap;
  }
  
  @Override
  public Map<String, Analyzer> getFieldAnalyzerMap() {
    return this.fieldFieldAnalyzerMap;
  }

  public String getName() {
    return name;
  }

  public String[] getFieldNames() {
    return fieldNames.toArray(new String[fieldNames.size()]);
  }

  @Override
  public String getRegionPath() {
    return region.getFullPath();
  }

  @Override
  public XmlGenerator<Region<?, ?>> getXmlGenerator() {
    return new LuceneIndexXmlGenerator(this);
  }

  @Override
  public void onCreate(Extensible<Region<?, ?>> source,
      Extensible<Region<?, ?>> target) {
    target.getExtensionPoint().addExtension(this);
    Cache cache = target.getExtensionPoint().getTarget().getCache();
    LuceneServiceImpl service = (LuceneServiceImpl) LuceneServiceProvider.get(cache);
    Region region = target.getExtensionPoint().getTarget();
    String aeqId = LuceneServiceImpl.getUniqueIndexName(getName(), getRegionPath());
    //Here, it is safe to add the aeq with the mutator, because onCreate is
    //fired in a special place before the region is initialized.
    //TODO - this may only work for PRs. We need to intercept the attributes
    //before the region is created with a RegionListener.
    region.getAttributesMutator().addAsyncEventQueueId(aeqId);
    service.afterDataRegionCreated(getName(), new StandardAnalyzer(), getRegionPath(), getFieldNames());
  }

  public void addField(String name) {
    this.fieldNames.add(name);
  }

  public void addFieldNames(String[] fieldNames) {
    this.fieldNames.addAll(Arrays.asList(fieldNames));
    
  }
}
