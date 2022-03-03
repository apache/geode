/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.cache;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.geode.DataSerializer;
import org.apache.geode.internal.Assert;

/**
 * Used to create subclasses that wrap another callback argument by having a reference to the
 * original and adding some data of their own. Customers should never see a callback arg that is an
 * instance of this class. It is for internal purposes only.
 *
 *
 * @since GemFire 5.7
 */
public abstract class WrappedCallbackArgument {

  /**
   * The original callbackArg
   */
  private Object _originalCallbackArg;

  /**
   * No arg constructor for DataSerializable.
   */
  public WrappedCallbackArgument() {}

  /**
   * Constructor.
   *
   * @param originalCallbackArg The original callback argument set by the caller or null if there
   *        was not callback arg
   */
  public WrappedCallbackArgument(Object originalCallbackArg) {
    _originalCallbackArg = originalCallbackArg;
  }


  /**
   * Returns the original callback argument.
   *
   * @return the original callback argument
   */
  public Object getOriginalCallbackArg() {
    return _originalCallbackArg;
  }

  public void toData(DataOutput out) throws IOException {
    DataSerializer.writeObject(_originalCallbackArg, out);
  }

  public void fromData(DataInput in) throws IOException, ClassNotFoundException {
    _originalCallbackArg = DataSerializer.readObject(in);
  }

  void setOriginalCallbackArgument(Object origCallbackArg) {
    Assert.assertTrue(_originalCallbackArg == null);
    _originalCallbackArg = origCallbackArg;
  }
}
