/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
 * @since GemFire 6.1
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
