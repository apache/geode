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

import java.util.Collection;
import java.util.Collections;

/**
 * Encapsulates a {@link org.apache.geode.cache.operations.OperationContext.OperationCode#REMOVEALL}
 * operation for both the pre-operation and post-operation cases.
 *
 * @since GemFire 8.1
 * @deprecated since Geode1.0, use {@link org.apache.geode.security.ResourcePermission} instead
 */
public class RemoveAllOperationContext extends OperationContext {

  /** The collection of keys for the operation */
  private final Collection<?> keys;

  /** True if this is a post-operation context */
  private boolean postOperation = false;

  private Object callbackArg;

  /**
   * Constructor for the operation.
   *
   */
  public RemoveAllOperationContext(Collection<?> keys) {
    this.keys = keys;
  }

  /**
   * Return the operation associated with the <code>OperationContext</code> object.
   *
   * @return <code>OperationCode.RemoveAll</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.REMOVEALL;
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
  protected void setPostOperation() {
    postOperation = true;
  }

  /**
   * Returns the keys for this removeAll in an unmodifiable collection.
   */
  public Collection<?> getKeys() {
    return Collections.unmodifiableCollection(keys);
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
