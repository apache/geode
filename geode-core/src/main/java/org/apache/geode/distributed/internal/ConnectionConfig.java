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
package org.apache.geode.distributed.internal;

import org.apache.geode.distributed.internal.membership.api.QuorumChecker;

/**
 * Contains the distribution config and internal properties for connecting.
 */
public interface ConnectionConfig {
  /**
   * Indicates whether this config is reconnecting.
   */
  boolean isReconnecting();

  /**
   * Returns the quorum checker to use while reconnecting.
   */
  QuorumChecker quorumChecker();

  /**
   * Returns the distribution config.
   */
  DistributionConfig distributionConfig();
}
