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
package org.apache.geode.cache.client.internal.locator.wan;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.admin.remote.DistributionLocatorId;

/**
 * A listener to handle membership when new locator is added to remote locator metadata. This
 * listener is expected to inform all other locators in remote locator metadata about the new
 * locator so that they can update their remote locator metadata.
 *
 *
 */
public interface LocatorMembershipListener {

  Object handleRequest(Object request);

  void setPort(int port);

  void setConfig(DistributionConfig config);

  /**
   * When the new locator is added to remote locator metadata, inform all other locators in remote
   * locator metadata about the new locator so that they can update their remote locator metadata.
   *
   */
  void locatorJoined(int distributedSystemId, DistributionLocatorId locator,
      DistributionLocatorId sourceLocator);

  Set<String> getRemoteLocatorInfo(int dsId);

  ConcurrentMap<Integer, Set<DistributionLocatorId>> getAllLocatorsInfo();

  ConcurrentMap<Integer, Set<String>> getAllServerLocatorsInfo();

  void clearLocatorInfo();

  void start();

  void stop();
}
