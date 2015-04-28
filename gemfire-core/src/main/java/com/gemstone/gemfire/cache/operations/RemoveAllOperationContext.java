/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;

import java.util.Collection;
import java.util.Collections;

import com.gemstone.gemfire.cache.operations.OperationContext;

/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#REMOVEALL} operation for both the
 * pre-operation and post-operation cases.
 * 
 * @author Darrel Schneider
 * @since 8.1
 */
public class RemoveAllOperationContext extends OperationContext {

  /** The collection of keys for the operation */
  private Collection<?> keys;
  
  /** True if this is a post-operation context */
  private boolean postOperation = false;
  
  private Object callbackArg;

  /**
   * Constructor for the operation.
   * 
   */
  public RemoveAllOperationContext(Collection<?> keys) {
    this.keys = keys;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.RemoveAll</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.REMOVEALL;
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
  protected void setPostOperation() {
    this.postOperation = true;
  }

  /**
   * Returns the keys for this removeAll in an unmodifiable collection.
   */
  public Collection<?> getKeys() {
    return Collections.unmodifiableCollection(this.keys);
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
