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
    this.haQueueCapacity = DEFAULT_CAPACITY;
    this.haEvictionPolicy = DEFAULT_EVICTION_POLICY;
    this.overflowDirectory = DEFAULT_OVERFLOW_DIRECTORY;
  }

  /**
   * Returns the capacity of the client client queue. will be in MB for eviction-policy mem else
   * number of entries
   *
   * @see #DEFAULT_CAPACITY
   * @since GemFire 5.7
   */
  public int getCapacity() {
    return this.haQueueCapacity;
  }

  /**
   * Sets the capacity of the client client queue. will be in MB for eviction-policy mem else number
   * of entries
   *
   * @see #DEFAULT_CAPACITY
   * @since GemFire 5.7
   */
  public void setCapacity(int capacity) {
    this.haQueueCapacity = capacity;
  }

  /**
   * Returns the eviction policy that is executed when capacity of the client client queue is
   * reached.
   *
   * @see #DEFAULT_EVICTION_POLICY
   * @since GemFire 5.7
   */
  public String getEvictionPolicy() {
    return this.haEvictionPolicy;
  }

  /**
   * Sets the eviction policy that is executed when capacity of the client client queue is reached.
   *
   * @see #DEFAULT_EVICTION_POLICY
   * @since GemFire 5.7
   */
  public void setEvictionPolicy(String policy) {
    this.haEvictionPolicy = policy;
  }

  /**
   * Sets the overflow directory for a client client queue
   *
   * @param overflowDirectory the overflow directory for a client queue's overflowed entries
   * @since GemFire 5.7
   * @deprecated as of prPersistSprint2
   */
  @Deprecated
  public void setOverflowDirectory(String overflowDirectory) {
    if (this.getDiskStoreName() != null) {
      throw new IllegalStateException(
          String.format("Deprecated API %s cannot be used with DiskStore %s",
              new Object[] {"setOverflowDirectory", this.getDiskStoreName()}));
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
  @Deprecated
  public String getOverflowDirectory() {
    if (this.getDiskStoreName() != null) {
      throw new IllegalStateException(
          String.format("Deprecated API %s cannot be used with DiskStore %s",
              new Object[] {"getOverflowDirectory", this.getDiskStoreName()}));
    }
    return this.overflowDirectory;
  }

  @Override
  public String toString() {
    String str = " Eviction policy " + this.getEvictionPolicy() + " capacity " + this.getCapacity();
    if (diskStoreName == null) {
      str += " Overflow Directory " + this.getOverflowDirectory();
    } else {
      str += " DiskStore Name: " + this.diskStoreName;
    }
    return str;
  }

  /**
   * get the diskStoreName for overflow
   *
   * @since GemFire prPersistSprint2
   */
  public String getDiskStoreName() {
    return diskStoreName;
  }

  /**
   * Sets the disk store name for overflow
   *
   * @since GemFire prPersistSprint2
   */
  public void setDiskStoreName(String diskStoreName) {
    if (hasOverflowDirectory()) {
      throw new IllegalStateException(
          String.format("Deprecated API %s cannot be used with DiskStore %s",
              new Object[] {"setDiskStoreName", this.getDiskStoreName()}));
    }
    this.diskStoreName = diskStoreName;
  }

  public boolean hasOverflowDirectory() {
    return this.hasOverflowDirectory;
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
