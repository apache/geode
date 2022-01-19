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

import org.apache.geode.cache.EntryOperation;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;

/**
 *
 * Gemfire Context passed to <code>PartitionResolver</code> to compute the data location
 *
 * @since GemFire 5.8
 *
 */
public class EntryOperationImpl implements EntryOperation {

  private final Region region;

  private final Operation operation;

  private final Object value;

  private final Object key;

  private final Object callbackArgument;

  public EntryOperationImpl(Region region, Operation operation, Object key, Object value,
      Object callbackArgument) {
    this.region = region;
    this.operation = operation;
    this.key = key;
    this.value = value;
    this.callbackArgument = callbackArgument;
  }

  /**
   * Returns the region to which this cached object belongs or the region that raised this event for
   * <code>RegionEvent</code>s.
   *
   * @return the region associated with this object or the region that raised this event.
   */
  @Override
  public Region getRegion() {
    return region;
  }

  /**
   * Return a description of the operation that triggered this event.
   *
   * @return the operation that triggered this event.
   * @since GemFire 5.8Beta
   */
  @Override
  public Operation getOperation() {
    return operation;
  }

  /**
   * Returns the key.
   *
   * @return the key
   */
  @Override
  public Object getKey() {
    return key;
  }

  @Override
  public Object getCallbackArgument() {
    Object result = callbackArgument;
    if (result == Token.NOT_AVAILABLE) {
      result = AbstractRegion.handleNotAvailable(result);
    } else if (result instanceof WrappedCallbackArgument) {
      WrappedCallbackArgument wca = (WrappedCallbackArgument) result;
      result = wca.getOriginalCallbackArg();
    }
    return result;
    // return AbstractRegion.handleNotAvailable(this.callbackArgument);
  }

  @Override
  public boolean isCallbackArgumentAvailable() {
    return callbackArgument != Token.NOT_AVAILABLE;
  }

  @Override
  public Object getNewValue() {
    return value;
  }

  public Object getRawNewValue() {
    return value;
  }
}
