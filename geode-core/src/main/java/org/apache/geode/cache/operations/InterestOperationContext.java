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

package org.apache.geode.cache.operations;


/**
 * Encapsulates registration/unregistration of interest in a region.
 *
 * @since GemFire 5.5
 * @deprecated since Geode1.0, use {@link org.apache.geode.security.ResourcePermission} instead
 */
public abstract class InterestOperationContext extends OperationContext {

  /** The key or list of keys being registered/unregistered. */
  private Object key;

  /** The {@link InterestType} of the operation. */
  private final InterestType interestType;

  /**
   * Constructor for the register interest operation.
   *
   * @param key the key or list of keys being registered/unregistered
   * @param interestType the <code>InterestType</code> of the register request
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
    return key;
  }

  /**
   * Set the key for this register/unregister interest operation.
   *
   * @param key the new key
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
    return interestType;
  }

}
