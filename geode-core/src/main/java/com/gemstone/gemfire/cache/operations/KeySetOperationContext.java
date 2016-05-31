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

import java.util.Set;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.operations.internal.ResourceOperationContext;

/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#KEY_SET} operation for both the
 * pre-operation and post-operation cases.
 * 
 * @since GemFire 5.5
 */
public class KeySetOperationContext extends ResourceOperationContext {

  /** The set of keys for the operation */
  private Set keySet;

  /**
   * Constructor for the operation.
   * 
   * @param postOperation
   *                true to set the post-operation flag
   */
  public KeySetOperationContext(boolean postOperation) {
    super(Resource.DATA, OperationCode.KEY_SET, postOperation);
  }

  /**
   * Get the set of keys returned as a result of {@link Region#keySet}
   * operation.
   * 
   * @return the set of keys
   */
  public Set getKeySet() {
    return this.keySet;
  }

  /**
   * Set the keys to be returned as the result of {@link Region#keySet}
   * operation.
   * 
   * @param keySet
   *                the set of keys to be returned for this operation.
   */
  public void setKeySet(Set keySet) {
    this.keySet = keySet;
  }

}
