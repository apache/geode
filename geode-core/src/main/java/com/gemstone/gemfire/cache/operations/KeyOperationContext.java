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

package com.gemstone.gemfire.cache.operations;

import com.gemstone.gemfire.cache.operations.internal.ResourceOperationContext;

/**
 * Encapsulates a region operation that requires only a key object for the
 * pre-operation case. The operations this class encapsulates are
 * {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#DESTROY} 
 * and {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#CONTAINS_KEY}.
 * 
 * @since GemFire 5.5
 */
public abstract class KeyOperationContext extends ResourceOperationContext {

  /** The key object of the operation */
  private Object key;

  /** Callback object for the operation (if any) */
  private Object callbackArg;

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   */
  public KeyOperationContext(OperationCode code, Object key) {
    this(code, key, false);
  }

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   * @param postOperation
   *                true to set the post-operation flag
   */
  protected KeyOperationContext(OperationCode code, Object key, boolean postOperation) {
    super(Resource.DATA, code, postOperation);
    this.key = key;
    this.callbackArg = null;
  }

  /**
   * Get the key object for this operation.
   * 
   * @return the key object for this operation.
   */
  public Object getKey() {
    return this.key;
  }

  /**
   * Get the callback argument object for this operation.
   * 
   * @return the callback argument object for this operation.
   */
  public Object getCallbackArg() {
    return this.callbackArg;
  }

  /**
   * Set the callback argument object for this operation.
   * 
   * @param callbackArg
   *                the callback argument object for this operation.
   */
  public void setCallbackArg(Object callbackArg) {
    this.callbackArg = callbackArg;
  }

}
