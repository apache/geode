/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;


/**
 * Encapsulates a region operation that requires only a key object for the
 * pre-operation case. The operations this class encapsulates are
 * {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#DESTROY} 
 * and {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#CONTAINS_KEY}.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public abstract class KeyOperationContext extends OperationContext {

  /** The key object of the operation */
  private Object key;

  /** Callback object for the operation (if any) */
  private Object callbackArg;

  /** True if this is a post-operation context */
  private boolean postOperation;

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   */
  public KeyOperationContext(Object key) {
    this.key = key;
    this.callbackArg = null;
    this.postOperation = false;
  }

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   * @param postOperation
   *                true to set the post-operation flag
   */
  public KeyOperationContext(Object key, boolean postOperation) {
    this.key = key;
    this.callbackArg = null;
    this.postOperation = postOperation;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return The <code>OperationCode</code> of this operation. This is one of
   *         {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#DESTROY} 
   *         or {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#CONTAINS_KEY}
   *         for <code>KeyOperationContext</code>, and one of
   *         {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#GET} or 
   *         {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#PUT} for
   *         <code>KeyValueOperationContext</code>.
   */
  @Override
  public abstract OperationCode getOperationCode();

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
  protected void setPostOperation() {
    this.postOperation = true;
  }

  /**
   * Get the key object for this operation.
   * 
   * @return the key object for this operation.
   */
  public Object getKey() {
    return this.key;
  }

  /**
   * Get the callback argument object for this operation.
   * 
   * @return the callback argument object for this operation.
   */
  public Object getCallbackArg() {
    return this.callbackArg;
  }

  /**
   * Set the callback argument object for this operation.
   * 
   * @param callbackArg
   *                the callback argument object for this operation.
   */
  public void setCallbackArg(Object callbackArg) {
    this.callbackArg = callbackArg;
  }

}
