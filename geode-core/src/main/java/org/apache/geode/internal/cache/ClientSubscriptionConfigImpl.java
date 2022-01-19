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
package org.apache.geode.internal.cache;

import org.apache.geode.cache.server.ClientSubscriptionConfig;

/**
 *
 * Configuration parameters for client subscription
 */
public class ClientSubscriptionConfigImpl implements ClientSubscriptionConfig {

  /**
   * To get client subscription
   */
  public static final String CLIENT_SUBSCRIPTION = "client_subscription";

  private int haQueueCapacity = 1;

  private String haEvictionPolicy = null;

  /**
   * The name of the directory in which to store overflowed client queue entries
   */
  private String overflowDirectory;

  /**
   * disk store name for overflow
   */
  private String diskStoreName;

  private boolean hasOverflowDirectory = false;

  public ClientSubscriptionConfigImpl() {
    haQueueCapacity = DEFAULT_CAPACITY;
    haEvictionPolicy = DEFAULT_EVICTION_POLICY;
    overflowDirectory = DEFAULT_OVERFLOW_DIRECTORY;
  }

  /**
   * Returns the capacity of the client client queue. will be in MB for eviction-policy mem else
   * number of entries
   *
   * @see #DEFAULT_CAPACITY
   * @since GemFire 5.7
   */
  @Override
  public int getCapacity() {
    return haQueueCapacity;
  }

  /**
   * Sets the capacity of the client client queue. will be in MB for eviction-policy mem else number
   * of entries
   *
   * @see #DEFAULT_CAPACITY
   * @since GemFire 5.7
   */
  @Override
  public void setCapacity(int capacity) {
    haQueueCapacity = capacity;
  }

  /**
   * Returns the eviction policy that is executed when capacity of the client client queue is
   * reached.
   *
   * @see #DEFAULT_EVICTION_POLICY
   * @since GemFire 5.7
   */
  @Override
  public String getEvictionPolicy() {
    return haEvictionPolicy;
  }

  /**
   * Sets the eviction policy that is executed when capacity of the client client queue is reached.
   *
   * @see #DEFAULT_EVICTION_POLICY
   * @since GemFire 5.7
   */
  @Override
  public void setEvictionPolicy(String policy) {
    haEvictionPolicy = policy;
  }

  /**
   * Sets the overflow directory for a client client queue
   *
   * @param overflowDirectory the overflow directory for a client queue's overflowed entries
   * @since GemFire 5.7
   * @deprecated as of prPersistSprint2
   */
  @Override
  @Deprecated
  public void setOverflowDirectory(String overflowDirectory) {
    if (getDiskStoreName() != null) {
      throw new IllegalStateException(
          String.format("Deprecated API %s cannot be used with DiskStore %s",
              "setOverflowDirectory", getDiskStoreName()));
    }
    this.overflowDirectory = overflowDirectory;
    setHasOverflowDirectory(true);
  }

  /**
   * Answers the overflow directory for a client queue's overflowed client queue entries.
   *
   * @return the overflow directory for a client queue's overflowed entries
   * @since GemFire 5.7
   * @deprecated as of prPersistSprint2
   */
  @Override
  @Deprecated
  public String getOverflowDirectory() {
    if (getDiskStoreName() != null) {
      throw new IllegalStateException(
          String.format("Deprecated API %s cannot be used with DiskStore %s",
              "getOverflowDirectory", getDiskStoreName()));
    }
    return overflowDirectory;
  }

  @Override
  public String toString() {
    String str = " Eviction policy " + getEvictionPolicy() + " capacity " + getCapacity();
    if (diskStoreName == null) {
      str += " Overflow Directory " + getOverflowDirectory();
    } else {
      str += " DiskStore Name: " + diskStoreName;
    }
    return str;
  }

  /**
   * get the diskStoreName for overflow
   *
   * @since GemFire prPersistSprint2
   */
  @Override
  public String getDiskStoreName() {
    return diskStoreName;
  }

  /**
   * Sets the disk store name for overflow
   *
   * @since GemFire prPersistSprint2
   */
  @Override
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

  /*
   * public boolean equals(ClientSubscriptionConfig other) { if (other != null &&
   * other.getEvictionPolicy() != null && other.getOverflowDirectory() != null) { if
   * ((this.getEvictionPolicy() == other.getEvictionPolicy()) && (this.getOverflowDirectory() ==
   * other.getOverflowDirectory()) && (this.getCapacity() == this.getCapacity())) return true; }
   * return false; }
   */
}
