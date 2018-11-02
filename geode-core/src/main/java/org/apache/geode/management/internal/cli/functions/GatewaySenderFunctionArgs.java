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
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.DeclarableType;


public class GatewaySenderFunctionArgs implements Serializable {
  private static final long serialVersionUID = 4636678328980816780L;

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
  // array of fully qualified class names of the filters
  private final List<String> gatewayEventFilters;
  private final List<String> gatewayTransportFilters;

  public GatewaySenderFunctionArgs(CacheConfig.GatewaySender sender) {
    this.id = sender.getId();
    this.remoteDSId = string2int(sender.getRemoteDistributedSystemId());
    this.parallel = sender.isParallel();
    this.manualStart = sender.isManualStart();
    this.socketBufferSize = string2int(sender.getSocketBufferSize());
    this.socketReadTimeout = string2int(sender.getSocketReadTimeout());
    this.enableBatchConflation = sender.isEnableBatchConflation();
    this.batchSize = string2int(sender.getBatchSize());
    this.batchTimeInterval = string2int(sender.getBatchTimeInterval());
    this.enablePersistence = sender.isEnablePersistence();
    this.diskStoreName = sender.getDiskStoreName();
    this.diskSynchronous = sender.isDiskSynchronous();
    this.maxQueueMemory = string2int(sender.getMaximumQueueMemory());
    this.alertThreshold = string2int(sender.getAlertThreshold());
    this.dispatcherThreads = string2int(sender.getDispatcherThreads());
    this.orderPolicy = sender.getOrderPolicy();
    this.gatewayEventFilters =
        Optional.of(sender.getGatewayEventFilters())
            .map(filters -> filters
                .stream().map(DeclarableType::getClassName)
                .collect(Collectors.toList()))
            .orElse(null);
    this.gatewayTransportFilters =
        Optional.of(sender.getGatewayTransportFilters())
            .map(filters -> filters
                .stream().map(DeclarableType::getClassName)
                .collect(Collectors.toList()))
            .orElse(null);
  }

  private Integer string2int(String x) {
    return Optional.ofNullable(x).map(Integer::valueOf).orElse(null);
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

  public List<String> getGatewayEventFilter() {
    return this.gatewayEventFilters;
  }

  public List<String> getGatewayTransportFilter() {
    return this.gatewayTransportFilters;
  }
}
