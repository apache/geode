/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

import java.io.Serializable;

/***
 * Data class used to pass index related information to 
 * functions that create or destroy indexes
 * @author bansods
 *
 */
public class IndexInfo implements Serializable {

  private static final long serialVersionUID = 1L;
  public final static int RANGE_INDEX = 1;
  public final static int KEY_INDEX = 2;
  public final static int HASH_INDEX = 3;

  private String indexName;
  private String indexedExpression = null;
  private String regionPath = null;
  private int indexType = RANGE_INDEX;

  public IndexInfo(String indexName) {
    this.indexName = indexName;
  }
  
  /***
   * Used for passing index information for destroying index.
   * @param indexName
   * @param regionPath
   */
  public IndexInfo (String indexName, String regionPath) {
    this.indexName = indexName;
    this.regionPath = regionPath;
  }
  public IndexInfo(String indexName, String indexedExpression, String regionPath) {
    this.indexName = indexName;
    this.indexedExpression = indexedExpression;
    this.regionPath = regionPath;
  }

  public IndexInfo(String indexName, String indexedExpression, String regionPath, int indexType) {
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
  public int getIndexType() {
    return indexType;
  }
  public void setIndexType(int indexType) {
    this.indexType = indexType;
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
    sb.append(this.indexType);
    return sb.toString();
  }

}
