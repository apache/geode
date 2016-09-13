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


/**
 * Encapsulates a {@link com.gemstone.gemfire.cache.operations.OperationContext.OperationCode#GET} region operation having the key
 * object for the pre-operation case and both key, value objects for the
 * post-operation case.
 * 
 * @since GemFire 5.5
 */
public class GetOperationContext extends KeyValueOperationContext {

  /**
   * Constructor for the operation.
   * 
   * @param key
   *                the key for this operation
   * @param postOperation
   *                true if the context is for the post-operation case
   */
  public GetOperationContext(Object key, boolean postOperation) {
    super(key, null, false, postOperation);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code>
   * object.
   * 
   * @return <code>OperationCode.GET</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.GET;
  }

  /**
   * Set the post-operation flag to true.
   */
  @Override
  public void setPostOperation() {
    super.setPostOperation();
  }

  /**
   * Get the value of this get operation.
   * 
   * @return the result of get operation; null when the result is a serialized
   *         value in which case user should invoke {@link #getSerializedValue()}
   *         or {@link #getDeserializedValue()}.
   */
  public Object getObject() {
    if (super.getSerializedValue() != null) {
      return null;
    } else {
      return super.getValue();
    }
  }

  /**
   * Set the result value of the object for this operation.
   * 
   * @param value
   *                the result of this operation; can be a serialized byte array
   *                or a deserialized object
   * @param isObject
   *                true when the value is an object (either serialized or
   *                deserialized); false when it is a raw byte array
   */
  public void setObject(Object value, boolean isObject) {
    super.setValue(value, isObject);
  }

}
