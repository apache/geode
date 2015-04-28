/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.domain;

/**
 * Used to transfer information about an AsyncEventQueue from a function
 * being executed on a server back to the manager that invoked the function.
 * 
 * @author David Hoots
 * @since 8.0
 */
import java.io.Serializable;
import java.util.Properties;

public class AsyncEventQueueDetails implements Serializable {
  private static final long serialVersionUID = 1L;
  private final String id;
  private final int batchSize;
  private final boolean persistent;
  private final String diskStoreName;
  private final int maxQueueMemory;
  private final String listener;
  private final Properties listenerProperties;

  public AsyncEventQueueDetails(final String id, final int batchSize, final boolean persistent, final String diskStoreName,
      final int maxQueueMemory, final String listener, final Properties listenerProperties) {
    this.id = id;
    this.batchSize = batchSize;
    this.persistent = persistent;
    this.diskStoreName = diskStoreName;
    this.maxQueueMemory = maxQueueMemory;
    this.listener = listener;
    this.listenerProperties = listenerProperties;
  }

  public String getId() {
    return this.id;
  }

  public int getBatchSize() {
    return this.batchSize;
  }

  public boolean isPersistent() {
    return this.persistent;
  }

  public String getDiskStoreName() {
    return this.diskStoreName;
  }

  public int getMaxQueueMemory() {
    return this.maxQueueMemory;
  }

  public String getListener() {
    return this.listener;
  }
  
  public Properties getListenerProperties() {
    return this.listenerProperties;
  }
}
