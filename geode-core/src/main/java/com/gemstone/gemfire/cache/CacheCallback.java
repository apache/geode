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

package com.gemstone.gemfire.cache;

/**
 * User-defined objects that can be plugged into caching to receive callback
 * notifications.
 *
 *
 * @since 3.0
 */
public interface CacheCallback {
  /** Called when the region containing this callback is closed or destroyed, when
   * the cache is closed, or when a callback is removed from a region
   * using an <code>AttributesMutator</code>.
   *
   * <p>Implementations should cleanup any external
   * resources such as database connections. Any runtime exceptions this method
   * throws will be logged.
   *
   * <p>It is possible for this method to be called multiple times on a single
   * callback instance, so implementations must be tolerant of this.
   *
   * @see Cache#close()
   * @see Region#close
   * @see Region#localDestroyRegion()
   * @see Region#destroyRegion()
   * @see AttributesMutator
   */
  public void close();
}
