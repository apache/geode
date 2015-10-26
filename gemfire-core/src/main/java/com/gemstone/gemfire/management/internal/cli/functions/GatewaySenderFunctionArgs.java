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


public class GatewaySenderFunctionArgs implements Serializable {
  private static final long serialVersionUID = -5158224572470173267L;

  private final String id;
  private final Integer remoteDSId;
  private final Boolean parallel;
  private final Boolean manualStart;
  private final Integer socketBufferSize;
  private final Integer socketReadTimeout;
  private final Boolean enableBatchConflation;
  private final Integer batchSize;
  private final Integer batchTimeInterval;
  private final Boolean enablePersistence;
  private final String diskStoreName;
  private final Boolean diskSynchronous;
  private final Integer maxQueueMemory;
  private final Integer alertThreshold;
  private final Integer dispatcherThreads;
  private final String orderPolicy;
  //array of fully qualified class names of the filters
  private final String[] gatewayEventFilters;
  private final String[] gatewayTransportFilters;
  
  public GatewaySenderFunctionArgs(String id,
      Integer remoteDSId, Boolean parallel, Boolean manualStart, Integer socketBufferSize, 
      Integer socketReadTimeout, Boolean enableBatchConflation, Integer batchSize, 
      Integer batchTimeInterval, Boolean enablePersistence, String diskStoreName, 
      Boolean diskSynchronous, Integer maxQueueMemory, Integer alertThreshold, 
      Integer dispatcherThreads, String orderPolicy, String[] gatewayEventFilters, 
      String[] gatewayTransportFilters) {
    
    this.id = id;
    this.remoteDSId = remoteDSId;
    this.parallel = parallel;
    this.manualStart = manualStart;
    this.socketBufferSize = socketBufferSize;
    this.socketReadTimeout = socketReadTimeout;
    this.enableBatchConflation = enableBatchConflation;
    this.batchSize = batchSize;
    this.batchTimeInterval = batchTimeInterval;
    this.enablePersistence = enablePersistence;
    this.diskStoreName = diskStoreName;
    this.diskSynchronous = diskSynchronous;
    this.maxQueueMemory = maxQueueMemory;
    this.alertThreshold = alertThreshold;
    this.dispatcherThreads = dispatcherThreads;
    this.orderPolicy = orderPolicy;
    this.gatewayEventFilters = gatewayEventFilters;
    this.gatewayTransportFilters = gatewayTransportFilters;
  }
  
  public String getId() {
    return this.id;
  }
  
  public Integer getRemoteDistributedSystemId() {
    return this.remoteDSId;
  }
  
  public Boolean isParallel() {
    return this.parallel;
  }
  
  public Boolean isManualStart() {
    return this.manualStart;
  }
  
  public Integer getSocketBufferSize() {
    return this.socketBufferSize;
  }
  
  public Integer getSocketReadTimeout() {
    return this.socketReadTimeout;
  }
  
  public Boolean isBatchConflationEnabled() {
    return this.enableBatchConflation;
  }
  
  public Integer getBatchSize() {
    return this.batchSize;
  }
  
  public Integer getBatchTimeInterval() {
    return this.batchTimeInterval;
  }
  
  public Boolean isPersistenceEnabled() {
    return this.enablePersistence;
  }
  
  public String getDiskStoreName() {
    return this.diskStoreName;
  }
  
  public Boolean isDiskSynchronous() {
    return this.diskSynchronous;
  }
  
  public Integer getMaxQueueMemory() {
    return this.maxQueueMemory;
  }
  
  public Integer getAlertThreshold() {
    return this.alertThreshold;
  }
  
  public Integer getDispatcherThreads() {
    return this.dispatcherThreads;
  }
  
  public String getOrderPolicy() {
    return this.orderPolicy;
  }
  
  public String[] getGatewayEventFilter() {
    return this.gatewayEventFilters;
  }
  
  public String[] getGatewayTransportFilter() {
    return this.gatewayTransportFilters;
  }
}
