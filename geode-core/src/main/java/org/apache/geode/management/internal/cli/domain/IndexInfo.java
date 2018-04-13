/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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
package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;

import org.apache.geode.cache.query.IndexType;

/***
 * Data class used to pass index related information to functions that create or destroy indexes
 *
 */
public class IndexInfo implements Serializable {

  private static final long serialVersionUID = 1L;

  private String indexName;
  private String indexedExpression = null;
  private String regionPath = null;
  private IndexType indexType = IndexType.FUNCTIONAL;
  private boolean ifExists;

  public IndexInfo(String indexName) {
    this.indexName = indexName;
  }

  /***
   * Used for passing index information for destroying index.
   *
   */
  public IndexInfo(String indexName, String regionPath) {
    this.indexName = indexName;
    this.regionPath = regionPath;
  }

  public IndexInfo(String indexName, String indexedExpression, String regionPath) {
    this.indexName = indexName;
    this.indexedExpression = indexedExpression;
    this.regionPath = regionPath;
  }

  public IndexInfo(String indexName, String indexedExpression, String regionPath,
      IndexType indexType) {
    this.indexName = indexName;
    this.indexedExpression = indexedExpression;
    this.regionPath = regionPath;
    this.indexType = indexType;
  }

  public String getIndexName() {
    return indexName;
  }

  public void setIndexName(String indexName) {
    this.indexName = indexName;
  }

  public String getIndexedExpression() {
    return indexedExpression;
  }

  public void setIndexedExpression(String indexedExpression) {
    this.indexedExpression = indexedExpression;
  }

  public String getRegionPath() {
    return this.regionPath;
  }

  public void setRegionPath(String regionPath) {
    this.regionPath = regionPath;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public void setIndexType(IndexType indexType) {
    this.indexType = indexType;
  }

  public boolean isIfExists() {
    return ifExists;
  }

  public void setIfExists(boolean ifExists) {
    this.ifExists = ifExists;
  }

  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Index Name : ");
    sb.append(this.indexName);
    sb.append("\nIndexed Expression : ");
    sb.append(this.indexedExpression);
    sb.append("\nRegion Path : ");
    sb.append(this.regionPath);
    sb.append("\nIndex Type : ");
    sb.append(this.indexType.getName());
    return sb.toString();
  }

}
