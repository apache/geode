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

import java.util.Map;

import org.apache.geode.cache.operations.internal.UpdateOnlyMap;

/**
 * Encapsulates a {@link org.apache.geode.cache.operations.OperationContext.OperationCode#PUTALL}
 * operation for both the pre-operation and post-operation cases.
 *
 * @since GemFire 5.7
 * @deprecated since Geode1.0, use {@link org.apache.geode.security.ResourcePermission} instead
 */
public class PutAllOperationContext extends OperationContext {

  /** The set of keys for the operation */
  private final UpdateOnlyMap map;

  /** True if this is a post-operation context */
  private boolean postOperation = false;

  private Object callbackArg;

  /**
   * Constructor for the operation.
   *
   */
  public PutAllOperationContext(Map map) {
    this.map = new UpdateOnlyMap(map);
  }

  /**
   * Return the operation associated with the <code>OperationContext</code> object.
   *
   * @return <code>OperationCode.PUTALL</code>.
   */
  @Override
  public OperationCode getOperationCode() {
    return OperationCode.PUTALL;
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
   * Returns the map whose keys and values will be put. Note that only the values of this map can be
   * changed. You can not add or remove keys. Any attempt to modify the returned map with an
   * operation that is not supported will throw an UnsupportedOperationException. If the returned
   * map is modified and this is a pre-operation authorization then the modified map is what will be
   * used by the operation.
   */
  public <K, V> Map<K, V> getMap() {
    return map;
  }

  /**
   * Set the authorized map.
   *
   * @throws IllegalArgumentException if the given map is null or if its keys are not the same as
   *         the original keys.
   * @deprecated use getMap() instead and modify the values in the map it returns
   */
  public void setMap(Map map) {
    if (map == this.map) {
      return;
    }
    if (map == null) {
      throw new IllegalArgumentException(
          "PutAllOperationContext.setMap does not allow a null map.");
    }
    if (map.size() != this.map.size()) {
      throw new IllegalArgumentException(
          "PutAllOperationContext.setMap does not allow the size of the map to be changed.");
    }
    // this.map is a LinkedHashMap and our implementation needs its order to be preserved.
    // So take each entry from the input "map" and update the corresponding entry in the linked
    // "this.map".
    // Note that updates do not change the order of a linked hash map; only inserts do.
    try {
      this.map.putAll(map);
    } catch (UnsupportedOperationException ex) {
      throw new IllegalArgumentException("PutAllOperationContext.setMap " + ex.getMessage()
          + " to the original keys of the putAll");
    }
  }

  /**
   * Get the callback argument object for this operation.
   *
   * @return the callback argument object for this operation.
   * @since GemFire 8.1
   */
  public Object getCallbackArg() {
    return callbackArg;
  }

  /**
   * Set the callback argument object for this operation.
   *
   * @param callbackArg the callback argument object for this operation.
   * @since GemFire 8.1
   */
  public void setCallbackArg(Object callbackArg) {
    this.callbackArg = callbackArg;
  }
}
