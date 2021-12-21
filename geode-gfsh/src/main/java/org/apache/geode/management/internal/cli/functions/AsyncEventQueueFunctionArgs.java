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
package org.apache.geode.management.internal.cli.functions;

import java.io.Serializable;
import java.util.Properties;

/**
 * This class stores the arguments provided for create async event queue command.
 */
public class AsyncEventQueueFunctionArgs implements Serializable {

  private static final long serialVersionUID = -6524494645663740872L;

  private final String asyncEventQueueId;
  private final boolean isParallel;
  private final boolean enableBatchConflation;
  private final int batchSize;
  private final int batchTimeInterval;
  private final boolean persistent;
  private final String diskStoreName;
  private final boolean diskSynchronous;
  private final int maxQueueMemory;
  private final int dispatcherThreads;
  private final String orderPolicy;
  private final String[] gatewayEventFilters;
  private final String gatewaySubstitutionFilter;
  private final String listenerClassName;
  private final Properties listenerProperties;
  private final boolean forwardExpirationDestroy;

  public AsyncEventQueueFunctionArgs(String asyncEventQueueId, boolean isParallel,
      boolean enableBatchConflation, int batchSize, int batchTimeInterval, boolean persistent,
      String diskStoreName, boolean diskSynchronous, int maxQueueMemory, int dispatcherThreads,
      String orderPolicy, String[] gatewayEventFilters, String gatewaySubstitutionFilter,
      String listenerClassName, Properties listenerProperties, boolean forwardExpirationDestroy) {
    this.asyncEventQueueId = asyncEventQueueId;
    this.isParallel = isParallel;
    this.enableBatchConflation = enableBatchConflation;
    this.batchSize = batchSize;
    this.batchTimeInterval = batchTimeInterval;
    this.persistent = persistent;
    this.diskStoreName = diskStoreName;
    this.diskSynchronous = diskSynchronous;
    this.maxQueueMemory = maxQueueMemory;
    this.dispatcherThreads = dispatcherThreads;
    this.orderPolicy = orderPolicy;
    this.gatewayEventFilters = gatewayEventFilters;
    this.gatewaySubstitutionFilter = gatewaySubstitutionFilter;
    this.listenerClassName = listenerClassName;
    this.listenerProperties = listenerProperties;
    this.forwardExpirationDestroy = forwardExpirationDestroy;
  }

  public String getAsyncEventQueueId() {
    return asyncEventQueueId;
  }

  public boolean isParallel() {
    return isParallel;
  }

  public boolean isEnableBatchConflation() {
    return enableBatchConflation;
  }

  public int getBatchSize() {
    return batchSize;
  }

  public int getBatchTimeInterval() {
    return batchTimeInterval;
  }

  public boolean isPersistent() {
    return persistent;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public boolean isDiskSynchronous() {
    return diskSynchronous;
  }

  public int getMaxQueueMemory() {
    return maxQueueMemory;
  }

  public int getDispatcherThreads() {
    return dispatcherThreads;
  }

  public String getOrderPolicy() {
    return orderPolicy;
  }

  public String[] getGatewayEventFilters() {
    return gatewayEventFilters;
  }

  public String getGatewaySubstitutionFilter() {
    return gatewaySubstitutionFilter;
  }

  public String getListenerClassName() {
    return listenerClassName;
  }

  public Properties getListenerProperties() {
    return listenerProperties;
  }

  public boolean isForwardExpirationDestroy() {
    return forwardExpirationDestroy;
  }
}
