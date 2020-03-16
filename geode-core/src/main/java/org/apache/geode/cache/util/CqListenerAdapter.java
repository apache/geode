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

package org.apache.geode.cache.util;

/*
 * Abstract class for CqListener. Utility class that implements all methods in
 * <code>CqListener</code> with empty implementations. Applications can subclass this class and only
 * override the methods of interest.
 *
 * @since GemFire 5.1
 */

import org.apache.geode.cache.query.CqEvent;
import org.apache.geode.cache.query.CqListener;

public abstract class CqListenerAdapter implements CqListener {

  /**
   * An event occurred that modifies the results of the query. This event does not contain an error.
   */
  @Override
  public void onEvent(CqEvent aCqEvent) {}

  /**
   * An error occurred in the processing of a CQ. This event does contain an error. The newValue and
   * oldValue in the event may or may not be available, and will be null if not available.
   */
  @Override
  public void onError(CqEvent aCqEvent) {}

  /**
   * Called when the CQ is closed, the base region is destroyed, when the cache is closed, or when
   * this listener is removed from a CqQuery using a <code>CqAttributesMutator</code>.
   *
   * <p>
   * Implementations should cleanup any external resources such as database connections. Any runtime
   * exceptions this method throws will be logged.
   *
   * <p>
   * It is possible for this method to be called multiple times on a single callback instance, so
   * implementations must be tolerant of this.
   *
   * @see org.apache.geode.cache.CacheCallback#close
   */
  @Override
  public void close() {}
}
