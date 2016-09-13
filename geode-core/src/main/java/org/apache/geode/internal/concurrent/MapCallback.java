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

import java.util.concurrent.ConcurrentMap;

/**
 * Factory to create a value on demand for custom map <code>create</code>
 * methods rather than requiring a pre-built object as in
 * {@link ConcurrentMap#putIfAbsent(Object, Object)} that may be ultimately
 * thrown away.
 * <p>
 * Now also has a bunch of callbacks including for replace/remove etc.
 * 
 * @since GemFire 7.0
 * 
 * @param <K>
 *          the type of key of the map
 * @param <V>
 *          the type of value of the map
 * @param <C>
 *          the type of context parameter passed to the creation/removal methods
 * @param <P>
 *          the type of extra parameter passed to the creation/removal methods
 */
public interface MapCallback<K, V, C, P> {

  /**
   * Token to return from {@link #removeValue} to indicate that remove has to be
   * aborted.
   */
  public static final Object ABORT_REMOVE_TOKEN = new Object();

  /**
   * Token object to indicate that {@link #removeValue} does not need to compare
   * against provided value before removing from segment.
   */
  public static final Object NO_OBJECT_TOKEN = new Object();

  /**
   * Create a new instance of the value object given the key and provided
   * parameters for construction. Invoked by the <code>create</code> method in
   * custom map impls.
   * 
   * @param key
   *          the key for which the value is being created
   * @param context
   *          any context in which this method has been invoked
   * @param createParams
   *          parameters, if any, required for construction of a new value
   *          object
   * 
   * @return the new value to be inserted in the map
   */
  public V newValue(K key, C context, P createParams, MapResult result);

  /**
   * Invoked when an existing value in map is read by the <code>create</code>
   * method in custom map impls.
   * 
   * @param key
   *          the key for which the value is being created
   * @param oldValue
   *          the value read by create that will be returned
   * @param context
   *          any context in which this method has been invoked
   * @param params
   *          parameters, if any, required for construction of value
   * 
   * @return updated value to be put in the map, if non-null; if null then retry
   *         the map operation internally
   */
  public V updateValue(K key, V oldValue, C context, P params);

  /**
   * Invoked after put is successful with the result of {@link #updateValue}.
   * 
   * @param key
   *          the key provided to the put() operation
   * @param mapKey
   *          the existing key in the map
   * @param newValue
   *          the new value to be replaced
   * @param context
   *          any callback argument passed to put overload
   */
  public void afterUpdate(K key, K mapKey, V newValue, C context);

  /**
   * Returns true if {@link #updateValue} should be invoked else false.
   */
  public boolean requiresUpdateValue();

  /**
   * Invoked when an existing value in map is read by read ops.
   * 
   * @param oldValue
   *          the value read by create that will be returned
   */
  public void oldValueRead(V oldValue);

  /**
   * Check if the existing value should be removed by the custom
   * <code>remove</code> methods that take MapCallback as argument.
   * 
   * If this method returns null, then proceed with remove as usual, if this
   * method returns {@link #ABORT_REMOVE_TOKEN} then don't do the remove, and if
   * this method returns any other object then replace the map value instead of
   * removing.
   * 
   * @param key
   *          the key of the entry to be removed from the map
   * @param value
   *          the value to be removed
   * @param existingValue
   *          the current value in the map
   * @param context
   *          any context in which this method has been invoked
   * @param removeParams
   *          parameters, if any, to be passed for cleanup of the object
   */
  public Object removeValue(Object key, Object value, V existingValue,
      C context, P removeParams);

  /**
   * Invoked after removal of an entry. Some implementations
   * (CustomEntryConcurrentHashMap) will invoke this for both successful or
   * failed removal (in latter case existingValue will be null) while others
   * invoke this only for successful remove.
   * 
   * @param key
   *          the key of the entry removed from the map; some implementations
   *          (ConcurrentSkipListMap) will return the actual key in the map
   *          while others will provide the passed key
   * @param value
   *          the value to be removed sent to the two argument remove
   * @param existingValue
   *          the actual value in map being removed
   * @param context
   *          any context in which this method has been invoked
   * @param removeParams
   *          parameters, if any, to be passed for cleanup of the object
   */
  public void postRemove(Object key, Object value, V existingValue, C context,
      P removeParams);

  /**
   * Invoked when an existing value in map is read by the <code>replace</code>
   * method in custom map impls, and is to be replaced by a new value.
   * 
   * @param key
   *          the key for which the value is being replaced
   * @param oldValue
   *          the old value passed to replace method
   * @param existingValue
   *          the current value in the map
   * @param newValue
   *          the new value passed to replace method
   * @param context
   *          any context in which this method has been invoked
   * @param params
   *          parameters, if any, required for construction of a value
   * 
   * @return updated value to be actually put in the map, if non-null; if null
   *         then retry the map operation internally
   */
  public V replaceValue(K key, V oldValue, V existingValue, V newValue,
      C context, P params);

  /**
   * Invoked after the node is found and just before the replace. The replace
   * may still either succeed or fail.
   * 
   * @param mapKey
   *          the existing key in the map
   * @param newValue
   *          the new value to be replaced
   * @param context
   *          any context argument passed to replace
   * @param params
   *          any callback parameters passed to the replace method
   */
  public Object beforeReplace(K mapKey, V newValue, C context, P params);

  /**
   * Invoked after replace is successful and passing it the result of
   * {@link #beforeReplace}.
   * 
   * @param mapKey
   *          the existing key in the map
   * @param newValue
   *          the new value to be replaced
   * @param beforeResult
   *          the result of {@link #beforeReplace}
   * @param context
   *          any context argument passed to replace overload
   * @param params
   *          any callback parameters passed to the replace method
   */
  public void afterReplace(K mapKey, V newValue, Object beforeResult,
      C context, P params);

  /**
   * Invoked after replace fails and passing it the result of
   * {@link #beforeReplace}.
   * 
   * @param mapKey
   *          the existing key in the map
   * @param newValue
   *          the new value to be replaced
   * @param beforeResult
   *          the result of {@link #beforeReplace}
   * @param context
   *          any context argument passed to replace overload
   * @param params
   *          any callback parameters passed to the replace method
   */
  public void onReplaceFailed(K mapKey, V newValue, Object beforeResult,
      C context, P params);

  /**
   * Invoked after replace or delete fails at the end, passing it the
   * intermediate values that were returned by {@link #replaceValue},
   * {@link #updateValue} or {@link #removeValue} so it can revert any
   * side-affects of those methods.
   * 
   * @param key
   *          the key for which the operation failed
   * @param oldValue
   *          the expected oldValue passed for a replace or remove operation
   * @param updatedValue
   *          the new replacement value for the map that failed (possibly
   *          changed by {@link #replaceValue}, {@link #updateValue} or
   *          {@link #removeValue} callbacks)
   * @param newValue
   *          the new value initially sent to the replace method; if null then
   *          it is for a delete or insert operation
   * @param context
   *          any context argument passed to the original method
   * @param params
   *          any callback parameters passed to the original method
   * 
   * @return value to be returned as result of operation ignoring failure (or
   *         null for failure)
   */
  public V onOperationFailed(K key, Object oldValue, V updatedValue,
      V newValue, C context, P params);

  /**
   * Invoked by some implementations like ConcurrentTHashSet to in its toArray.
   */
  public void onToArray(C context);
}
