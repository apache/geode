/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;

/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#REGION_CREATE} operation for both the
 * pre-operation and post-operation cases.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class RegionCreateOperationContext extends OperationContext {

  /** True if this is a post-operation context */
  private boolean postOperation;

  /**
   * Constructor for the region creation operation.
   * 
   * @param postOperation
   *                true to set the post-operation flag
   */
  public RegionCreateOperationContext(boolean postOperation) {
    this.postOperation = postOperation;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.REGION_CREATE</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.REGION_CREATE;
  }

  /**
   * True if the context is for post-operation.
   */
  @Override
  public boolean isPostOperation() {
    return this.postOperation;
  }

}
