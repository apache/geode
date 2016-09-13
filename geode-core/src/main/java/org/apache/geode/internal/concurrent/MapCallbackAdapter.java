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

package org.apache.geode.internal.concurrent;

/**
 * Simple adapter class providing empty default implementations for
 * {@link MapCallback}.
 */
public class MapCallbackAdapter<K, V, C, P> implements MapCallback<K, V, C, P> {

  /**
   * @see MapCallback#newValue
   */
  @Override
  public V newValue(K key, C context, P createParams, MapResult result) {
    return null;
  }

  /**
   * @see MapCallback#updateValue
   */
  @Override
  public V updateValue(K key, V oldValue, C context, P params) {
    return oldValue;
  }

  /**
   * @see MapCallback#afterUpdate
   */
  @Override
  public void afterUpdate(K key, K mapKey, V newValue, C context) {
  }

  /**
   * @see MapCallback#requiresUpdateValue
   */
  @Override
  public boolean requiresUpdateValue() {
    return false;
  }

  /**
   * @see MapCallback#oldValueRead
   */
  @Override
  public void oldValueRead(V oldValue) {
  }

  /**
   * @see MapCallback#removeValue
   */
  @Override
  public Object removeValue(Object key, Object value, V existingValue,
      C context, P removeParams) {
    if (value != null
        && (value == NO_OBJECT_TOKEN || value.equals(existingValue))) {
      return null;
    }
    else {
      return ABORT_REMOVE_TOKEN;
    }
  }

  /**
   * @see MapCallback#postRemove
   */
  @Override
  public void postRemove(Object key, Object value, V existingValue, C context,
      P removeParams) {
  }

  /**
   * @see MapCallback#replaceValue
   */
  @Override
  public V replaceValue(K key, V oldValue, V existingValue, V newValue,
      C context, P params) {
    if (oldValue != null && oldValue.equals(existingValue)) {
      return newValue;
    }
    else {
      return null;
    }
  }

  /**
   * @see MapCallback#beforeReplace
   */
  @Override
  public Object beforeReplace(K mapKey, V newValue, C context, P params) {
    return null;
  }

  /**
   * @see MapCallback#afterReplace
   */
  @Override
  public void afterReplace(K mapKey, V newValue, Object beforeResult,
      C context, P params) {
  }

  /**
   * @see MapCallback#onReplaceFailed
   */
  @Override
  public void onReplaceFailed(K mapKey, V newValue, Object beforeResult,
      C context, P params) {
  }

  /**
   * @see MapCallback#onOperationFailed
   */
  @Override
  public V onOperationFailed(K key, Object oldValue, V updatedValue,
      V newValue, C context, P params) {
    return null;
  }

  /**
   * @see MapCallback#onToArray       
   */
  @Override
  public void onToArray(C context) {
  }
}
