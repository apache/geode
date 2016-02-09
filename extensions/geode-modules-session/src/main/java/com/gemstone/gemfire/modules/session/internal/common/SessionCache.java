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

package com.gemstone.gemfire.modules.session.internal.common;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.cache.Region;

import javax.servlet.http.HttpSession;

/**
 * Interface to basic cache operations.
 */
public interface SessionCache {

  /**
   * Initialize the cache and create the appropriate region.
   */
  public void initialize();

  /**
   * Stop the cache.
   */
  public void stop();

  /**
   * Retrieve the cache reference.
   *
   * @return a {@code GemFireCache} reference
   */
  public GemFireCache getCache();

  /**
   * Get the {@code Region} being used by client code to put attributes.
   *
   * @return a {@code Region<String, HttpSession>} reference
   */
  public Region<String, HttpSession> getOperatingRegion();

  /**
   * Get the backing {@code Region} being used. This may not be the same as the
   * region being used by client code to put attributes.
   *
   * @return a {@code Region<String, HttpSession>} reference
   */
  public Region<String, HttpSession> getSessionRegion();

  /**
   * Is this cache client-server? The only other alternative is peer-to-peer.
   *
   * @return true if this cache is client-server.
   */
  public boolean isClientServer();
}
