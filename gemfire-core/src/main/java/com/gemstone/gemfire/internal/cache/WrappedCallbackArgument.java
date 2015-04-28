/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializer;
import com.gemstone.gemfire.internal.Assert;

/**
 * Used to create subclasses that wrap another callback argument by having
 * a reference to the original and adding some data of their own.
 * Customers should never see a callback arg that is an instance of this class.
 * It is for internal purposes only.
 *
 * @author darrel
 *
 * @since 5.7 
 */
public abstract class WrappedCallbackArgument {

  /**
   * The original callbackArg
   */
  private Object _originalCallbackArg;
  
  /** If the GatewayEvent is in a Sql Fabric started Hub, in which case
   * the original callback argument is not serialized
   * 
   */
   private boolean serializeCallbackArg = true; 

  /**
   * No arg constructor for DataSerializable.
   */
  public WrappedCallbackArgument() {    
  }

  /**
   * Constructor.
   *
   * @param originalCallbackArg The original callback argument set by the
   * caller or null if there was not callback arg
   * @param serializeCBArg  boolean indicating if the event is created by a 
   * sql fabric system
   */
  public WrappedCallbackArgument(Object originalCallbackArg, boolean serializeCBArg) {
    this._originalCallbackArg = originalCallbackArg;
    this.serializeCallbackArg = serializeCBArg;
  }
 
  
  /**
   * Constructor.
   *
   * @param originalCallbackArg The original callback argument set by the
   * caller or null if there was not callback arg
   */
  public WrappedCallbackArgument(Object originalCallbackArg) {
    this._originalCallbackArg = originalCallbackArg;    
  }


  /**
   * Returns the original callback argument.
   * @return the original callback argument
   */
  public Object getOriginalCallbackArg() {
    return this._originalCallbackArg;
  }

  public void toData(DataOutput out) throws IOException {
    if(this.serializeCallbackArg) {
      DataSerializer.writeObject(this._originalCallbackArg, out);
    }else {
      DataSerializer.writeObject(null, out);      
    }
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    this._originalCallbackArg = DataSerializer.readObject(in);
  }
  
  void setOriginalCallbackArgument(Object origCallbackArg) {
    Assert.assertTrue(this._originalCallbackArg == null);
    this._originalCallbackArg = origCallbackArg;
  }
}
