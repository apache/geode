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

import java.util.Set;

import org.apache.geode.cache.Region;

/**
 * Encapsulates a {@link org.apache.geode.cache.operations.OperationContext.OperationCode#KEY_SET}
 * operation for both the pre-operation and post-operation cases.
 *
 * @since GemFire 5.5
 * @deprecated since Geode1.0, use {@link org.apache.geode.security.ResourcePermission} instead
 */
public class KeySetOperationContext extends OperationContext {

  /** The set of keys for the operation */
  private Set keySet;

  /** True if this is a post-operation context */
  private boolean postOperation;

  /**
   * Constructor for the operation.
   *
   * @param postOperation true to set the post-operation flag
   */
  public KeySetOperationContext(boolean postOperation) {
    this.postOperation = postOperation;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code> object.
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
    return postOperation;
  }

  /**
   * Set the post-operation flag to true.
   */
  public void setPostOperation() {
    postOperation = true;
  }

  /**
   * Get the set of keys returned as a result of {@link Region#keySet} operation.
   *
   * @return the set of keys
   */
  public Set getKeySet() {
    return keySet;
  }

  /**
   * Set the keys to be returned as the result of {@link Region#keySet} operation.
   *
   * @param keySet the set of keys to be returned for this operation.
   */
  public void setKeySet(Set keySet) {
    this.keySet = keySet;
  }

}
