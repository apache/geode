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
package org.apache.geode.management.internal;

import org.apache.geode.StatisticsFactory;
import org.apache.geode.annotations.VisibleForTesting;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.InternalCacheForClientAccess;
import org.apache.geode.internal.statistics.StatisticsClock;

/**
 * The Manager is a 7.0 JMX Agent which is hosted within a GemFire process. Only one instance is
 * allowed per DistributedSystem connection (or loner). It's responsible for defining JMX server
 * endpoints for servicing JMX clients.
 *
 * @since GemFire 7.0
 */
public abstract class Manager implements ManagerLifecycle {

  protected final InternalCacheForClientAccess cache;

  /**
   * This is a single window to manipulate region resources for management
   */
  protected final ManagementResourceRepo repo;

  /**
   * The concrete implementation of DistributedSystem that provides internal-only functionality.
   */
  protected final InternalDistributedSystem system;

  protected final StatisticsFactory statisticsFactory;

  protected final StatisticsClock statisticsClock;

  /**
   * True if this node is a Geode JMX manager.
   */
  protected volatile boolean running;

  protected volatile boolean stopCacheOps;

  Manager(ManagementResourceRepo repo, InternalDistributedSystem system, InternalCache cache,
      StatisticsFactory statisticsFactory, StatisticsClock statisticsClock) {
    this.repo = repo;
    this.cache = cache.getCacheForProcessingClientRequests();
    this.system = system;
    this.statisticsFactory = statisticsFactory;
    this.statisticsClock = statisticsClock;
  }

  @VisibleForTesting
  public ManagementResourceRepo getManagementResourceRepo() {
    return repo;
  }
}
