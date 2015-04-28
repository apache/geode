/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;

import java.util.Set;

import com.gemstone.gemfire.cache.operations.OperationContext;

/**
 * Encapsulates a cache query operation for both the pre-operation and
 * post-operation cases.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class QueryOperationContext extends OperationContext {

  /** The query string of this query operation. */
  private String queryString;

  /** The set of regions that are referred by the query string. */
  private Set regionNames;

  /** The result of query operation */
  private Object queryResult;

  /** True if this is a post-operation context */
  private boolean postOperation;

  /** The bind parameters for the query */
  private Object[] queryParams;
  
  /**
   * Constructor for the query operation.
   * 
   * @param queryString
   *                the query string for this operation
   * @param regionNames
   *                names of regions that are part of the query string
   * @param postOperation
   *                true to set the post-operation flag
   */
  public QueryOperationContext(String queryString, Set regionNames,
      boolean postOperation) {
    this.queryString = queryString;
    this.regionNames = regionNames;
    this.queryResult = null;
    this.postOperation = postOperation;
  }

  /**
   * Constructor for the query operation.
   * 
   * @param queryString
   *                the query string for this operation
   * @param regionNames
   *                names of regions that are part of the query string
   * @param postOperation
   *                true to set the post-operation flag
   * @param queryParams
   *                the bind parameters for the query
   */
  public QueryOperationContext(String queryString, Set regionNames,
      boolean postOperation, Object[] queryParams) {
    this(queryString, regionNames, postOperation);
    this.queryParams = queryParams;
  }
  
  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return the <code>OperationCode</code> of this operation
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.QUERY;
  }

  /**
   * True if the context is for post-operation.
   */
  @Override
  public boolean isPostOperation() {
    return this.postOperation;
  }

  /**
   * Set the post-operation flag to true.
   */
  public void setPostOperation() {
    this.postOperation = true;
  }

  /** Return the query string of this query operation. */
  public String getQuery() {
    return this.queryString;
  }

  /**
   * Modify the query string.
   * 
   * @param query
   *                the new query string for this query.
   */
  public void modifyQuery(String query) {
    this.queryString = query;
    this.regionNames = null;
  }

  /**
   * Get the names of regions that are part of the query string.
   * 
   * @return names of regions being queried.
   */
  public Set getRegionNames() {
    return this.regionNames;
  }

  /**
   * Set the names of regions that are part of the query string.
   * 
   * @param regionNames names of regions being queried
   */
  public void setRegionNames(Set regionNames) {
    this.regionNames = regionNames;
  }

  /**
   * Get the result of the query execution.
   * 
   * @return result of the query.
   */
  public Object getQueryResult() {
    return this.queryResult;
  }

  /**
   * Set the result of query operation.
   * 
   * @param queryResult
   *                the new result of the query operation.
   */
  public void setQueryResult(Object queryResult) {
    this.queryResult = queryResult;
  }

  /**
   * Get the bind parameters for the query
   * 
   * @return bind parameters for the query
   */
  public Object[] getQueryParams() {
    return queryParams;
  }

}
