/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.cache.lucene.internal.cli;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import com.gemstone.gemfire.cache.lucene.internal.LuceneIndexImpl;

public class LuceneIndexDetails implements Comparable<LuceneIndexDetails>, Serializable {

  private String indexName;
  private String regionPath;
  private String[] searchableFieldNames;
  protected Class<?> analyzer;
  private Map<String, Class<?>> fieldAnalyzers= new HashMap<String, Class<?>>();

  public LuceneIndexDetails(LuceneIndexImpl index) {
    indexName = index.getName();
    regionPath = index.getRegionPath();
    searchableFieldNames = index.getFieldNames();
    analyzer = index.getAnalyzer().getClass();
    setFieldAnalyzers(index);
  }

  private void setFieldAnalyzers(final LuceneIndexImpl index) {
    if (index.getFieldAnalyzers() != null) {
      for (Entry entry : index.getFieldAnalyzers().entrySet()) {
        fieldAnalyzers.put(entry.getKey().toString(), entry.getValue().getClass());
      }
    }
  }

  public String getSearchableFieldNamesString() {
    StringBuffer stringBuffer=new StringBuffer("[");
    if (searchableFieldNames !=null) {
      for (String field : searchableFieldNames) {
        stringBuffer.append(field + ",");
      }
      stringBuffer.deleteCharAt(stringBuffer.length()-1);
    }
    return stringBuffer.append("]").toString();
  }


  public String getFieldAnalyzersString() {
    StringBuffer stringBuffer=new StringBuffer("[");
    if (!fieldAnalyzers.isEmpty()) {
      for(Entry entry: fieldAnalyzers.entrySet()) {
        stringBuffer.append("<"+entry.getKey()+","+entry.getValue()+">,");
      }
      stringBuffer.deleteCharAt(stringBuffer.length()-1);
    }
    return stringBuffer.append("]").toString();
  }

  @Override
  public String toString() {
    final StringBuffer buffer = new StringBuffer();
    buffer.append("{\n\tIndex Name = "+indexName);
    buffer.append(",\tRegion Path = "+regionPath);
    buffer.append(",\tAnalyzer = "+analyzer);
    buffer.append(",\tIndexed Fields = "+getSearchableFieldNamesString());
    buffer.append(",\tField Analyzer = "+getFieldAnalyzersString());
    buffer.append("\n}\n");
    return buffer.toString();
  }


  public String getIndexName() {
    return indexName;
  }

  public String getRegionPath() {
    return regionPath;
  }

  public Map<String, Class<?>> getFieldAnalyzers() {
    return fieldAnalyzers;
  }

  public Class<?> getAnalyzer() {
    return analyzer;
  }

  public String[] getSearchableFieldNames() {
    return searchableFieldNames;
  }

  private static <T extends Comparable<T>> int compare(final T obj1, final T obj2) {
    return (obj1 == null && obj2 == null ? 0 : (obj1 == null ? 1 : (obj2 == null ? -1 : obj1.compareTo(obj2))));
  }

  @Override
  public int compareTo(final LuceneIndexDetails indexDetails) {
    int comparisonValue = compare(getIndexName(), indexDetails.getIndexName());
    return (comparisonValue != 0 ? comparisonValue : compare(getRegionPath(), indexDetails.getRegionPath()));
  }

}
