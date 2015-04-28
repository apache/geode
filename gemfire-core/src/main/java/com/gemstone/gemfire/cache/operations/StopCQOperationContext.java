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
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#STOP_CQ} operation for the pre-operation
 * case.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class StopCQOperationContext extends ExecuteCQOperationContext {

  /**
   * Constructor for the STOP_CQ operation.
   * 
   * @param cqName
   *                the name of the continuous query being stopped
   * @param queryString
   *                the query string for this operation
   * @param regionNames
   *                names of regions that are part of the query string
   */
  public StopCQOperationContext(String cqName, String queryString,
      Set regionNames) {
    super(cqName, queryString, regionNames, false);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.STOP_CQ</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.STOP_CQ;
  }

  /**
   * True if the context is for post-operation.
   */
  @Override
  public boolean isPostOperation() {
    return false;
  }

}
