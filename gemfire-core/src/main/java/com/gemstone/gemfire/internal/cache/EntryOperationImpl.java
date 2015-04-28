/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache;

import com.gemstone.gemfire.cache.EntryOperation;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;

/**
 * 
 * Gemfire Context passed to <code>PartitionedResolver</code> to compute the
 * data location
 * 
 * @author Yogesh Mahajan
 * @since 5.8
 * 
 */
public class EntryOperationImpl implements EntryOperation {

  private final Region region;

  private final Operation operation;

  private final Object value;

  private final Object key;

  private Object callbackArgument = Token.NOT_AVAILABLE;

  public EntryOperationImpl(Region region, Operation operation, Object key,
      Object value, Object callbackArgument) {
    this.region = region;
    this.operation = operation;
    this.key = key;
    this.value = value;
    this.callbackArgument = callbackArgument;
  }

  /**
   * Returns the region to which this cached object belongs or the region that
   * raised this event for <code>RegionEvent</code>s.
   * 
   * @return the region associated with this object or the region that raised
   *         this event.
   */
  public Region getRegion() {
    return this.region;
  }

  /**
   * Return a description of the operation that triggered this event.
   * 
   * @return the operation that triggered this event.
   * @since 5.8Beta
   */
  public Operation getOperation() {
    return this.operation;
  }

  /**
   * Returns the key.
   * 
   * @return the key
   */
  public Object getKey() {
    return this.key;
  }

  public Object getCallbackArgument() {
    Object result = this.callbackArgument;
    if (result == Token.NOT_AVAILABLE) {
      result = AbstractRegion.handleNotAvailable(result);
    }else if (result instanceof WrappedCallbackArgument) {
      WrappedCallbackArgument wca = (WrappedCallbackArgument)result;
      result = wca.getOriginalCallbackArg();
    }
    return result;
    //return AbstractRegion.handleNotAvailable(this.callbackArgument);
  }
  
  public boolean isCallbackArgumentAvailable() {
    return this.callbackArgument != Token.NOT_AVAILABLE;
  }

  public Object getNewValue() {
    return this.value;
  }

  public Object getRawNewValue() {
    return this.value;
  }

  /**
   * Method for internal use. (Used by SQLFabric)
   */
  public void setCallbackArgument(Object newCallbackArgument) {
    if (this.callbackArgument instanceof WrappedCallbackArgument) {
      ((WrappedCallbackArgument)this.callbackArgument)
          .setOriginalCallbackArgument(newCallbackArgument);
    }
    else {
      this.callbackArgument = newCallbackArgument;
    }
  }
}
