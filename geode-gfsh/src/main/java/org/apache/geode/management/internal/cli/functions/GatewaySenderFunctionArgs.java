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
import java.util.Collections;
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
  private final Boolean groupTransactionEvents;
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
  private final Boolean enforceThreadsConnectSameReceiver;

  public GatewaySenderFunctionArgs(CacheConfig.GatewaySender sender) {
    id = sender.getId();
    remoteDSId = string2int(sender.getRemoteDistributedSystemId());
    parallel = sender.isParallel();
    groupTransactionEvents = sender.mustGroupTransactionEvents();
    manualStart = sender.isManualStart();
    socketBufferSize = string2int(sender.getSocketBufferSize());
    socketReadTimeout = string2int(sender.getSocketReadTimeout());
    enableBatchConflation = sender.isEnableBatchConflation();
    batchSize = string2int(sender.getBatchSize());
    batchTimeInterval = string2int(sender.getBatchTimeInterval());
    enablePersistence = sender.isEnablePersistence();
    diskStoreName = sender.getDiskStoreName();
    diskSynchronous = sender.isDiskSynchronous();
    maxQueueMemory = string2int(sender.getMaximumQueueMemory());
    alertThreshold = string2int(sender.getAlertThreshold());
    dispatcherThreads = string2int(sender.getDispatcherThreads());
    orderPolicy = sender.getOrderPolicy();
    if (sender.areGatewayEventFiltersUpdated()) {
      gatewayEventFilters =
          Optional.of(sender.getGatewayEventFilters())
              .map(filters -> filters
                  .stream().map(DeclarableType::getClassName)
                  .collect(Collectors.toList()))
              .orElse(Collections.emptyList());
    } else {
      gatewayEventFilters = null;
    }

    gatewayTransportFilters =
        Optional.of(sender.getGatewayTransportFilters())
            .map(filters -> filters
                .stream().map(DeclarableType::getClassName)
                .collect(Collectors.toList()))
            .orElse(null);
    enforceThreadsConnectSameReceiver = sender.getEnforceThreadsConnectSameReceiver();
  }

  private Integer string2int(String x) {
    return Optional.ofNullable(x).map(Integer::valueOf).orElse(null);
  }

  public String getId() {
    return id;
  }

  public Integer getRemoteDistributedSystemId() {
    return remoteDSId;
  }

  public Boolean isParallel() {
    return parallel;
  }

  public Boolean mustGroupTransactionEvents() {
    return groupTransactionEvents;
  }

  public Boolean isManualStart() {
    return manualStart;
  }

  public Integer getSocketBufferSize() {
    return socketBufferSize;
  }

  public Integer getSocketReadTimeout() {
    return socketReadTimeout;
  }

  public Boolean isBatchConflationEnabled() {
    return enableBatchConflation;
  }

  public Integer getBatchSize() {
    return batchSize;
  }

  public Integer getBatchTimeInterval() {
    return batchTimeInterval;
  }

  public Boolean isPersistenceEnabled() {
    return enablePersistence;
  }

  public String getDiskStoreName() {
    return diskStoreName;
  }

  public Boolean isDiskSynchronous() {
    return diskSynchronous;
  }

  public Integer getMaxQueueMemory() {
    return maxQueueMemory;
  }

  public Integer getAlertThreshold() {
    return alertThreshold;
  }

  public Integer getDispatcherThreads() {
    return dispatcherThreads;
  }

  public String getOrderPolicy() {
    return orderPolicy;
  }

  public List<String> getGatewayEventFilter() {
    return gatewayEventFilters;
  }

  public List<String> getGatewayTransportFilter() {
    return gatewayTransportFilters;
  }

  public Boolean getEnforceThreadsConnectSameReceiver() {
    return enforceThreadsConnectSameReceiver;
  }
}
