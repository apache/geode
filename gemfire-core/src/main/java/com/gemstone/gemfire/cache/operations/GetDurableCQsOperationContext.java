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
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#GET_DURABLE_CQS} operation for the pre-operation
 * case.
 * 
 * @author jhuynh
 * @since 7.0
 */
public class GetDurableCQsOperationContext extends OperationContext {

  /**
   * Constructor for the GET_DURABLE_CQS operation.
   * 
  
   */
  public GetDurableCQsOperationContext() {
    super();
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.GET_DURABLE_CQS</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.GET_DURABLE_CQS;
  }

  /**
   * True if the context is for post-operation.
   */
  @Override
  public boolean isPostOperation() {
    return false;
  }

}
