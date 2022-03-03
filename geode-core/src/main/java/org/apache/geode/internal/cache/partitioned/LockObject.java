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
package org.apache.geode.internal.cache.partitioned;

public class LockObject {
  public Object key;
  public long lockedTimeStamp;
  private boolean removed;
  private boolean waiting = false;

  public LockObject(Object key, long lockedTimeStamp) {
    this.key = key;
    this.lockedTimeStamp = lockedTimeStamp;
  }

  public void waiting() {
    waiting = true;
  }

  public boolean isSomeoneWaiting() {
    return waiting;
  }

  /** Always updated when the monitor is held on this object */
  public void setRemoved() {
    removed = true;
  }

  /** Always checked when the monitor is held on this object */
  public boolean isRemoved() {
    return removed;
  }

  @Override
  public String toString() {
    return "LockObject [key=" + key + ", lockedTimeStamp=" + lockedTimeStamp + ", removed="
        + removed + ", waiting=" + waiting + "]";
  }
}
