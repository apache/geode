/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;

import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.operations.OperationContext;

/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#KEY_SET} operation for both the
 * pre-operation and post-operation cases.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public class KeySetOperationContext extends OperationContext {

  /** The set of keys for the operation */
  private Set keySet;

  /** True if this is a post-operation context */
  private boolean postOperation;

  /**
   * Constructor for the operation.
   * 
   * @param postOperation
   *                true to set the post-operation flag
   */
  public KeySetOperationContext(boolean postOperation) {
    this.postOperation = postOperation;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.KEY_SET</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.KEY_SET;
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

  /**
   * Get the set of keys returned as a result of {@link Region#keySet}
   * operation.
   * 
   * @return the set of keys
   */
  public Set getKeySet() {
    return this.keySet;
  }

  /**
   * Set the keys to be returned as the result of {@link Region#keySet}
   * operation.
   * 
   * @param keySet
   *                the set of keys to be returned for this operation.
   */
  public void setKeySet(Set keySet) {
    this.keySet = keySet;
  }

}
