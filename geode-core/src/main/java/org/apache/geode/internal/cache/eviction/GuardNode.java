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

class GuardNode implements EvictionNode {

  private EvictionNode next;

  private EvictionNode previous;

  @Override
  public int getEntrySize() {
    return 0;
  }

  @Override
  public EvictionNode next() {
    return next;
  }

  @Override
  public EvictionNode previous() {
    return previous;
  }

  @Override
  public void setEvicted() {
    // nothing
  }

  @Override
  public void setNext(EvictionNode next) {
    this.next = next;
  }

  @Override
  public void setPrevious(EvictionNode previous) {
    this.previous = previous;
  }

  @Override
  public void setRecentlyUsed(RegionEntryContext context) {
    // nothing
  }

  @Override
  public boolean isEvicted() {
    return false;
  }

  @Override
  public boolean isRecentlyUsed() {
    return false;
  }

  @Override
  public void unsetEvicted() {
    // nothing
  }

  @Override
  public void unsetRecentlyUsed() {
    // nothing
  }

  @Override
  public int updateEntrySize(EvictionController ccHelper) {
    return 0;
  }

  @Override
  public int updateEntrySize(EvictionController ccHelper, Object value) {
    return 0;
  }

  @Override
  public boolean isInUseByTransaction() {
    return false;
  }
}
