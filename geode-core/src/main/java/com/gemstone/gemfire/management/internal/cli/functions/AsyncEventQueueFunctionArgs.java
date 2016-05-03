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
package com.gemstone.gemfire.management.internal.cli.functions;

import java.io.Serializable;
import java.util.Properties;

/**
 * This class stores the arguments provided for create async event queue command.
 */
public class AsyncEventQueueFunctionArgs implements Serializable {
  
  private static final long serialVersionUID = -6524494645663740872L;

  private String asyncEventQueueId;
  private boolean isParallel;
  private boolean enableBatchConflation;
  private int batchSize;
  private int batchTimeInterval;
  private boolean persistent;
  private String diskStoreName;
  private boolean diskSynchronous;
  private int maxQueueMemory;
  private int dispatcherThreads; 
  private String orderPolicy;
  private String[] gatewayEventFilters;
  private String gatewaySubstitutionFilter;
  private String listenerClassName;
  private Properties listenerProperties;
  private boolean ignoreEvictionAndExpiration;

  public AsyncEventQueueFunctionArgs(String asyncEventQueueId,
      boolean isParallel, boolean enableBatchConflation, int batchSize,
      int batchTimeInterval, boolean persistent, String diskStoreName,
      boolean diskSynchronous, int maxQueueMemory, int dispatcherThreads,
      String orderPolicy, String[] gatewayEventFilters,
      String gatewaySubstitutionFilter, String listenerClassName,
      Properties listenerProperties, boolean ignoreEvictionAndExpiration) {
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
    this.ignoreEvictionAndExpiration = ignoreEvictionAndExpiration;
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

  public boolean isIgnoreEvictionAndExpiration() {
    return ignoreEvictionAndExpiration;
  }
}
