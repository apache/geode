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
package org.apache.geode.internal.cache.xmlcache;


/**
 * This class represents the data given for binding an overflow mechanism to a client subscription.
 * It encapsulates eviction policy, capacity and overflowDirectory. This object will get created for
 * every <b>client-subscription</b> tag
 *
 * @since GemFire 5.7
 */
public class ClientHaQueueCreation {

  private int haQueueCapacity = 0;

  private String haEvictionPolicy = null;

  private String overflowDirectory = null;

  private String diskStoreName;

  private boolean hasOverflowDirectory = false;

  public int getCapacity() {
    return haQueueCapacity;
  }

  public void setCapacity(int capacity) {
    haQueueCapacity = capacity;
  }

  public String getEvictionPolicy() {
    return haEvictionPolicy;
  }

  public void setEvictionPolicy(String policy) {
    haEvictionPolicy = policy;
  }

  /**
   * @deprecated as of prPersistSprint2
   */
  public String getOverflowDirectory() {
    if (getDiskStoreName() != null) {
      throw new IllegalStateException(
          String.format("Deprecated API %s cannot be used with DiskStore %s",
              "getOverflowDirectory", getDiskStoreName()));
    }
    return overflowDirectory;
  }

  /**
   * @deprecated as of prPersistSprint2
   */
  public void setOverflowDirectory(String overflowDirectory) {
    if (getDiskStoreName() != null) {
      throw new IllegalStateException(
          String.format("Deprecated API %s cannot be used with DiskStore %s",
              "setOverflowDirectory", getDiskStoreName()));
    }
    this.overflowDirectory = overflowDirectory;
    setHasOverflowDirectory(true);

  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public void setDiskStoreName(String diskStoreName) {
    if (hasOverflowDirectory()) {
      throw new IllegalStateException(
          String.format("Deprecated API %s cannot be used with DiskStore %s",
              "setDiskStoreName", getDiskStoreName()));
    }
    this.diskStoreName = diskStoreName;
  }

  public boolean hasOverflowDirectory() {
    return hasOverflowDirectory;
  }

  private void setHasOverflowDirectory(boolean hasOverflowDirectory) {
    this.hasOverflowDirectory = hasOverflowDirectory;
  }
}
