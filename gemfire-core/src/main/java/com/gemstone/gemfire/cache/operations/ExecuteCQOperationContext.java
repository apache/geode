/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;

import java.util.Set;

/**
 * Encapsulates a continuous query registeration operation for both the
 * pre-operation and post-operation cases.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class ExecuteCQOperationContext extends QueryOperationContext {

  /** The name of the continuous query being registered. */
  private String cqName;

  /**
   * Constructor for the EXECUTE_CQ operation.
   * 
   * @param cqName
   *                the name of the continuous query being registered
   * @param queryString
   *                the query string for this operation
   * @param regionNames
   *                names of regions that are part of the query string
   * @param postOperation
   *                true to set the post-operation flag
   */
  public ExecuteCQOperationContext(String cqName, String queryString,
      Set regionNames, boolean postOperation) {
    super(queryString, regionNames, postOperation);
    this.cqName = cqName;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return the <code>OperationCode</code> of this operation
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.EXECUTE_CQ;
  }

  /** Return the name of the continuous query. */
  public String getName() {
    return this.cqName;
  }

}
