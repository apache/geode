/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.cache.operations;


/**
 * Encapsulates registration/unregistration of interest in a region.
 * 
 * @author Sumedh Wale
 * @since 5.5
 */
public abstract class InterestOperationContext extends OperationContext {

  /** The key or list of keys being registered/unregistered. */
  private Object key;

  /** The {@link InterestType} of the operation. */
  private InterestType interestType;

  /**
   * Constructor for the register interest operation.
   * 
   * @param key
   *                the key or list of keys being registered/unregistered
   * @param interestType
   *                the <code>InterestType</code> of the register request
   */
  public InterestOperationContext(Object key, InterestType interestType) {
    this.key = key;
    this.interestType = interestType;
  }

  /**
   * True if the context is for post-operation.
   */
  @Override
  public boolean isPostOperation() {
    return false;
  }

  /**
   * Get the key for this register/unregister interest operation.
   * 
   * @return the key to be registered/unregistered.
   */
  public Object getKey() {
    return this.key;
  }

  /**
   * Set the key for this register/unregister interest operation.
   * 
   * @param key
   *                the new key
   */
  public void setKey(Object key) {
    this.key = key;
  }

  /**
   * Get the <code>InterestType</code> of this register/unregister operation.
   * 
   * @return the <code>InterestType</code> of this request.
   */
  public InterestType getInterestType() {
    return this.interestType;
  }

}
