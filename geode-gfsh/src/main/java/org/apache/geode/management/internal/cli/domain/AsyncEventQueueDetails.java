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
package org.apache.geode.management.internal.cli.domain;

import java.io.Serializable;
import java.util.Properties;

/**
 * Used to transfer information about an AsyncEventQueue from a function being executed on a server
 * back to the manager that invoked the function.
 *
 * @since GemFire 8.0
 */
public class AsyncEventQueueDetails implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String id;
  private final int batchSize;
  private final boolean persistent;
  private final String diskStoreName;
  private final int maxQueueMemory;
  private final String listener;
  private final Properties listenerProperties;
  private final boolean createdWithPausedEventProcessing;
  private final boolean pausedEventProcessing;

  public AsyncEventQueueDetails(final String id, final int batchSize, final boolean persistent,
      final String diskStoreName, final int maxQueueMemory, final String listener,
      final Properties listenerProperties, final boolean createdWithPausedEventProcessing,
      final boolean pausedEventProcessing) {
    this.id = id;
    this.batchSize = batchSize;
    this.persistent = persistent;
    this.diskStoreName = diskStoreName;
    this.maxQueueMemory = maxQueueMemory;
    this.listener = listener;
    this.listenerProperties = listenerProperties;
    this.createdWithPausedEventProcessing = createdWithPausedEventProcessing;
    this.pausedEventProcessing = pausedEventProcessing;
  }

  public String getId() {
    return id;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public boolean isPersistent() {
    return persistent;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public int getMaxQueueMemory() {
    return maxQueueMemory;
  }

  public String getListener() {
    return listener;
  }

  public Properties getListenerProperties() {
    return listenerProperties;
  }

  public boolean isCreatedWithPausedEventProcessing() {
    return createdWithPausedEventProcessing;
  }

  public boolean isPausedEventProcessing() {
    return pausedEventProcessing;
  }
}
