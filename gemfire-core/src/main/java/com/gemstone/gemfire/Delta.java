/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * This interface defines a contract between the application and GemFire that
 * allows GemFire to determine whether an application object contains a delta,
 * allows GemFire to extract the delta from an application object, and generate
 * a new application object by applying a delta to an existing application
 * object. The difference in object state is contained in the {@link DataOutput}
 * and {@link DataInput} parameters.
 * 
 * @since 6.1
 * 
 */
public interface Delta {

  /**
   * Returns true if this object has pending changes it can write out.
   */
  boolean hasDelta();

  /**
   * This method is invoked on an application object at the delta sender, if
   * GemFire determines the presence of a delta by calling
   * {@link Delta#hasDelta()} on the object. The delta is written to the
   * {@link DataOutput} object provided by GemFire.
   * 
   * Any delta state should be reset in this method.
   * 
   * @throws IOException
   */
  void toDelta(DataOutput out) throws IOException;


  /**
   * This method is invoked on an existing application object when an update is
   * received as a delta. This method throws an {@link InvalidDeltaException}
   * when the delta in the {@link DataInput} cannot be applied to the object.
   * GemFire automatically handles an {@link InvalidDeltaException} by
   * reattempting the update by sending the full application object.
   * 
   * @throws IOException
   * @throws InvalidDeltaException
   */
  void fromDelta(DataInput in) throws IOException, InvalidDeltaException;
}
