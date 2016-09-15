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

package org.apache.geode.cache.operations;


/**
 * Encapsulates a {@link org.apache.geode.cache.operations.OperationContext.OperationCode#INVALIDATE} region operation having the key
 * object for both the pre-operation case and for post-operation updates.
 * 
 * @since GemFire 5.5
 */
public class InvalidateOperationContext extends KeyOperationContext {

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   */
  public InvalidateOperationContext(Object key) {
    super(key);
  }

  /**
   * Constructor for the operation to use for post-operation in updates.
   * 
   * @param key
   *                the key for this operation
   * @param isPostOperation 
   *                true if the context is at the time of
   *                sending updates                 
   */
  public InvalidateOperationContext(Object key, boolean isPostOperation) {
    super(key, isPostOperation);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.INVALIDATE</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.INVALIDATE;
  }

}
