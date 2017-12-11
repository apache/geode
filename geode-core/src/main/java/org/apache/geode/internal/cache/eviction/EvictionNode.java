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
package org.apache.geode.internal.cache.eviction;

import org.apache.geode.internal.cache.RegionEntryContext;

public interface EvictionNode {

  void setNext(EvictionNode next);

  void setPrevious(EvictionNode previous);

  EvictionNode next();

  EvictionNode previous();

  /** compute the new entry size and return the delta from the previous entry size */
  int updateEntrySize(EvictionController ccHelper);

  /**
   * compute the new entry size and return the delta from the previous entry size
   *
   * @param value then entry's value
   * @since GemFire 6.1.2.9
   */
  int updateEntrySize(EvictionController ccHelper, Object value);

  int getEntrySize();

  boolean isRecentlyUsed();

  void setRecentlyUsed(RegionEntryContext context);

  void unsetRecentlyUsed();

  void setEvicted();

  void unsetEvicted();

  boolean isEvicted();

  boolean isInUseByTransaction();
}
