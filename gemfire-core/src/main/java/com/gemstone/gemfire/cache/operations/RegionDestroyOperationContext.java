/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;


/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#REGION_DESTROY} operation for both the
 * pre-operation and post-operation cases.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class RegionDestroyOperationContext extends RegionOperationContext {

  /**
   * Constructor for the region destroy operation.
   * 
   * @param postOperation
   *                true to set the post-operation flag
   */
  public RegionDestroyOperationContext(boolean postOperation) {
    super(postOperation);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.REGION_DESTROY</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.REGION_DESTROY;
  }

}
