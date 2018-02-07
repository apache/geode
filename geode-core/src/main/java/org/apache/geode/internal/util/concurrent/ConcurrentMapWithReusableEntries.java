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
package org.apache.geode.internal.util.concurrent;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

public interface ConcurrentMapWithReusableEntries<K, V> extends ConcurrentMap<K, V> {

  /**
   * Returns a {@link Set} view of the mappings contained in this map. The set is backed by the map,
   * so changes to the map are reflected in the set, and vice-versa. The set supports element
   * removal, which removes the corresponding mapping from the map, via the
   * <tt>Iterator.remove</tt>, <tt>Set.remove</tt>, <tt>removeAll</tt>, <tt>retainAll</tt>, and
   * <tt>clear</tt> operations. It does not support the <tt>add</tt> or <tt>addAll</tt> operations.
   *
   * <p>
   * The view's <tt>iterator</tt> is a "weakly consistent" iterator that will never throw
   * {@link java.util.ConcurrentModificationException}, and guarantees to traverse elements as they
   * existed upon construction of the iterator, and may (but is not guaranteed to) reflect any
   * modifications subsequent to construction.
   *
   * <p>
   * This set provides entries that are reused during iteration so caller cannot store the returned
   * <code>Map.Entry</code> objects.
   */
  Set<Map.Entry<K, V>> entrySetWithReusableEntries();

  /**
   * Clear the map. If any work needs to be done asynchronously then use the given executor.
   *
   * @param executor possibly null. If it is null and asynchronous work needs to be done then a new
   *        Thread is created.
   */
  void clearWithExecutor(Executor executor);
}
