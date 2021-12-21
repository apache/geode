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
/*
 * Created on Apr 18, 2005
 *
 *
 */
package org.apache.geode.internal.cache.xmlcache;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.query.Index;
import org.apache.geode.cache.query.IndexStatistics;
import org.apache.geode.cache.query.IndexType;

/**
 *
 * This class contains the information needed to create an index It will contain the callback data
 * between <index></index> invocation
 */
class IndexCreationData implements Index {

  private String name = null;
  private String indexType = null;
  private String fromClause = null;
  private String expression = null;
  private String importStr = null;

  IndexCreationData(String name) {
    this.name = name;
  }

  void setIndexType(String indexType) {
    this.indexType = indexType;
  }

  String getIndexType() {
    return indexType;
  }

  void setFunctionalIndexData(String fromClause, String expression, String importStr) {
    this.fromClause = fromClause;
    this.expression = expression;
    this.importStr = importStr;
  }

  void setPrimaryKeyIndexData(String field) {
    expression = field;
  }

  String getIndexFromClause() {
    return fromClause;
  }

  String getIndexExpression() {
    return expression;
  }

  String getIndexImportString() {
    return importStr;
  }

  String getIndexName() {
    return name;
  }

  /*
   * Implements Index methods so that IndexCreationData can be used similarly to generate XML. Note
   * there are two files named of IndexCreationData This one is used specifically for tests at this
   * time. Most method implementations are no-ops
   */
  @Override
  public String getName() {
    return name;
  }

  @Override
  public IndexType getType() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Region<?, ?> getRegion() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public IndexStatistics getStatistics() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getFromClause() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCanonicalizedFromClause() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getIndexedExpression() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCanonicalizedIndexedExpression() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getProjectionAttributes() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getCanonicalizedProjectionAttributes() {
    // TODO Auto-generated method stub
    return null;
  }
}
