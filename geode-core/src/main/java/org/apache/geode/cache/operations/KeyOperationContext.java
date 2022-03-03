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
 * Encapsulates a region operation that requires only a key object for the pre-operation case. The
 * operations this class encapsulates are
 * {@link org.apache.geode.cache.operations.OperationContext.OperationCode#DESTROY} and
 * {@link org.apache.geode.cache.operations.OperationContext.OperationCode#CONTAINS_KEY}.
 *
 * @since GemFire 5.5
 * @deprecated since Geode1.0, use {@link org.apache.geode.security.ResourcePermission} instead
 */
public abstract class KeyOperationContext extends OperationContext {

  /** The key object of the operation */
  private final Object key;

  /** Callback object for the operation (if any) */
  private Object callbackArg;

  /** True if this is a post-operation context */
  private boolean postOperation;

  /**
   * Constructor for the operation.
   *
   * @param key the key for this operation
   */
  public KeyOperationContext(Object key) {
    this.key = key;
    callbackArg = null;
    postOperation = false;
  }

  /**
   * Constructor for the operation.
   *
   * @param key the key for this operation
   * @param postOperation true to set the post-operation flag
   */
  public KeyOperationContext(Object key, boolean postOperation) {
    this.key = key;
    callbackArg = null;
    this.postOperation = postOperation;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code> object.
   *
   * @return The <code>OperationCode</code> of this operation. This is one of
   *         {@link org.apache.geode.cache.operations.OperationContext.OperationCode#DESTROY} or
   *         {@link org.apache.geode.cache.operations.OperationContext.OperationCode#CONTAINS_KEY}
   *         for <code>KeyOperationContext</code>, and one of
   *         {@link org.apache.geode.cache.operations.OperationContext.OperationCode#GET} or
   *         {@link org.apache.geode.cache.operations.OperationContext.OperationCode#PUT} for
   *         <code>KeyValueOperationContext</code>.
   */
  @Override
  public abstract OperationCode getOperationCode();

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
  protected void setPostOperation() {
    postOperation = true;
  }

  /**
   * Get the key object for this operation.
   *
   * @return the key object for this operation.
   */
  public Object getKey() {
    return key;
  }

  /**
   * Get the callback argument object for this operation.
   *
   * @return the callback argument object for this operation.
   */
  public Object getCallbackArg() {
    return callbackArg;
  }

  /**
   * Set the callback argument object for this operation.
   *
   * @param callbackArg the callback argument object for this operation.
   */
  public void setCallbackArg(Object callbackArg) {
    this.callbackArg = callbackArg;
  }

}
