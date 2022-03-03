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

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.lucene.LuceneIndexExistsException;
import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.LuceneServiceProvider;
import org.apache.geode.cache.lucene.internal.LuceneServiceImpl;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.logging.internal.log4j.api.LogService;

public class LuceneIndexCreation implements Extension<Region<?, ?>> {
  private Region region;
  private String name;
  private final Set<String> fieldNames = new LinkedHashSet<>();
  private Map<String, Analyzer> fieldAnalyzers;

  private static final Logger logger = LogService.getLogger();
  private LuceneSerializer luceneSerializer;


  public void setRegion(Region region) {
    this.region = region;
  }

  public void setName(String name) {
    this.name = name;
  }

  public void setFieldAnalyzers(Map<String, Analyzer> fieldAnalyzers) {
    this.fieldAnalyzers = fieldAnalyzers;
  }

  public Map<String, Analyzer> getFieldAnalyzers() {
    if (fieldAnalyzers == null) {
      fieldAnalyzers = new HashMap<>();
    }
    return fieldAnalyzers;
  }

  public String getName() {
    return name;
  }

  public String[] getFieldNames() {
    return fieldNames.toArray(new String[fieldNames.size()]);
  }

  public String getRegionPath() {
    return region.getFullPath();
  }

  public LuceneSerializer getLuceneSerializer() {
    return luceneSerializer;
  }

  public void setLuceneSerializer(LuceneSerializer luceneSerializer) {
    this.luceneSerializer = luceneSerializer;
  }

  @Override
  public XmlGenerator<Region<?, ?>> getXmlGenerator() {
    return new LuceneIndexXmlGenerator(this);
  }

  @Override
  public void beforeCreate(Extensible<Region<?, ?>> source, Cache cache) {
    LuceneServiceImpl service = (LuceneServiceImpl) LuceneServiceProvider.get(cache);
    Analyzer analyzer = fieldAnalyzers == null ? new StandardAnalyzer()
        : new PerFieldAnalyzerWrapper(new StandardAnalyzer(), fieldAnalyzers);
    try {
      service.createIndex(getName(), getRegionPath(), analyzer, fieldAnalyzers,
          getLuceneSerializer(), false, getFieldNames());
    } catch (LuceneIndexExistsException e) {
      logger
          .info(String.format("Ignoring duplicate index creation for Lucene index %s on region %s",
              e.getIndexName(), e.getRegionPath()));
    }
  }

  @Override
  public void onCreate(Extensible<Region<?, ?>> source, Extensible<Region<?, ?>> target) {}

  protected void addField(String name) {
    fieldNames.add(name);
  }

  protected void addFieldAndAnalyzer(String name, Analyzer analyzer) {
    fieldNames.add(name);
    getFieldAnalyzers().put(name, analyzer);
  }

  public void addFieldNames(String[] fieldNames) {
    this.fieldNames.addAll(Arrays.asList(fieldNames));
  }

}
