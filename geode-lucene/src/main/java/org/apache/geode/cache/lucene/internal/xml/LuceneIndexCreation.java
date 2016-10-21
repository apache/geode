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

package org.apache.geode.cache.lucene.internal.xml;

import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneIndex;
import org.apache.geode.cache.lucene.LuceneService;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;

public class LuceneIndexCreation implements LuceneIndex, Extension<Region<?, ?>> {
  private Region region;
  private String name;
  private Set<String> fieldNames = new LinkedHashSet<String>();
  private Map<String, Analyzer> fieldAnalyzers;


  public void setRegion(Region region) {
    this.region = region;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setFieldAnalyzers(Map<String, Analyzer> fieldAnalyzers) {
    this.fieldAnalyzers = fieldAnalyzers;
  }

  @Override
  public Map<String, Analyzer> getFieldAnalyzers() {
    if (this.fieldAnalyzers == null) {
      this.fieldAnalyzers = new HashMap<>();
    }
    return this.fieldAnalyzers;
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
  public void beforeCreate(Extensible<Region<?, ?>> source, Cache cache) {
    LuceneServiceImpl service = (LuceneServiceImpl) LuceneServiceProvider.get(cache);
    Analyzer analyzer = this.fieldAnalyzers == null ? new StandardAnalyzer()
        : new PerFieldAnalyzerWrapper(new StandardAnalyzer(), this.fieldAnalyzers);
    service.createIndex(getName(), getRegionPath(), analyzer, this.fieldAnalyzers, getFieldNames());
  }

  @Override
  public void onCreate(Extensible<Region<?, ?>> source, Extensible<Region<?, ?>> target) {}

  protected void addField(String name) {
    this.fieldNames.add(name);
  }

  protected void addFieldAndAnalyzer(String name, Analyzer analyzer) {
    this.fieldNames.add(name);
    getFieldAnalyzers().put(name, analyzer);
  }

  public void addFieldNames(String[] fieldNames) {
    this.fieldNames.addAll(Arrays.asList(fieldNames));
  }

  @Override
  public boolean waitUntilFlushed(int maxWaitInMillisecond) {
    return true;
  }
}
