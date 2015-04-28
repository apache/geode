/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/*
 * Created on Apr 18, 2005
 *
 * 
 */
package com.gemstone.gemfire.internal.cache.xmlcache;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.query.Index;
import com.gemstone.gemfire.cache.query.IndexStatistics;
import com.gemstone.gemfire.cache.query.IndexType;

/**
 * @author asifs
 * 
 * This class contains the information needed to create an index It will
 * contain the callback data between <index></index> invocation
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
    return this.indexType;
  }

  void setFunctionalIndexData(String fromClause, String expression,
      String importStr) {
    this.fromClause = fromClause;
    this.expression = expression;
    this.importStr = importStr;
  }

  void setPrimaryKeyIndexData(String field) {
    this.expression = field;
  }

  String getIndexFromClause() {
    return this.fromClause;
  }

  String getIndexExpression() {
    return this.expression;
  }

  String getIndexImportString() {
    return this.importStr;
  }

  String getIndexName() {
    return this.name;
  }

  /*
   * Implements Index methods so that IndexCreationData can be used similarly to generate XML.  Note there are two files named of IndexCreationData
   * This one is used specifically for tests at this time.  Most method implementations are no-ops
   */
  @Override
  public String getName() {
    return this.name;
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
