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
 *
 */

package org.apache.geode.management.client;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementService;
import org.apache.geode.management.api.ClusterManagementServiceConnectionConfig;
import org.apache.geode.management.api.ClusterManagementServiceTransport;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.internal.ClientClusterManagementService;

/**
 * This builder facilitates creating a ClusterManagementService using either (or both) a {@link
 * ClusterManagementServiceConnectionConfig} or a {@link ClusterManagementServiceTransport}. For
 * typical usage it should be sufficient to only use a
 * {@code ClusterManagementServiceConnectionConfig}.
 * For example:
 *
 * <pre>
 * ClusterManagementService service = new ClusterManagementServiceBuilder()
 *     .setConnectionConfig(new BasicClusterManagementServiceConnectionConfig("localhost", 7070))
 *     .build();
 * </pre>
 *
 * If no transport is set a default transport of
 * {@link RestTemplateClusterManagementServiceTransport}
 * will be used and configured with the provided config.
 */
@Experimental
public class ClusterManagementServiceBuilder {

  private ClusterManagementServiceTransport transport;

  private ClusterManagementServiceConnectionConfig connectionConfig;

  /**
   * Build a new {@link ClusterManagementService} instance
   *
   * @return a ClusterManagementService
   * @throws IllegalStateException if neither transport or config have been set
   */
  public ClusterManagementService build() {
    if (transport == null && connectionConfig == null) {
      throw new IllegalStateException(
          "Both transportBuilder and connectionConfig are null. Please configure with at least one of setTransportBuilder() or setConnectionConfig()");
    }

    if (transport == null) {
      transport = new RestTemplateClusterManagementServiceTransport(connectionConfig);
    }

    return new ClientClusterManagementService(transport);
  }

  public ClusterManagementServiceBuilder setTransport(
      ClusterManagementServiceTransport transport) {
    this.transport = transport;
    return this;
  }

  public ClusterManagementServiceBuilder setConnectionConfig(
      ClusterManagementServiceConnectionConfig connectionConfig) {
    this.connectionConfig = connectionConfig;
    return this;
  }
}
