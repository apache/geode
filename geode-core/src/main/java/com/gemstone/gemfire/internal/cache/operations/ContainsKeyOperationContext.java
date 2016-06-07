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

package com.gemstone.gemfire.internal.cache.operations;

import com.gemstone.gemfire.cache.operations.KeyOperationContext;

/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#CONTAINS_KEY} region operation having the
 * key object for the pre-operation case.
 * 
 * @since GemFire 5.5
 */
public class ContainsKeyOperationContext extends KeyOperationContext {

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   */
  public ContainsKeyOperationContext(Object key) {
    super(key);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.CONTAINS_KEY</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.CONTAINS_KEY;
  }

}
