/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;


/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#DESTROY} region operation having the key
 * object for both the pre-operation case and for post-operation updates.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class DestroyOperationContext extends KeyOperationContext {

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   */
  public DestroyOperationContext(Object key) {
    super(key);
  }

  /**
   * Constructor for the operation to use for post-operation in updates.
   * 
   * @param key
   *                the key for this operation
   * @param postOperation
   *                true to set the post-operation flag
   */
  public DestroyOperationContext(Object key, boolean postOperation) {
    super(key, postOperation);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.DESTROY</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.DESTROY;
  }

}
