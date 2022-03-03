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

package org.apache.geode.management.cluster.client;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.ClusterManagementServiceTransport;
import org.apache.geode.management.api.ConnectionConfig;
import org.apache.geode.management.api.RestTemplateClusterManagementServiceTransport;
import org.apache.geode.management.service.internal.api.BaseManagementServiceBuilder;

/**
 * This builder facilitates creating a ClusterManagementService using either (or both) a {@link
 * ConnectionConfig} or a {@link ClusterManagementServiceTransport}. For typical usage it should be
 * sufficient to only use a {@code ClusterManagementServiceConnectionConfig}. For example:
 *
 * <pre>
 * ClusterManagementService service = new ClusterManagementServiceBuilder()
 *     .setPort(7070)
 *     .setHost("localhost")
 *     .build();
 * </pre>
 * <p>
 * If no transport is set a public transport of
 * {@link RestTemplateClusterManagementServiceTransport}
 * will be used and configured with the provided config.
 */
@Experimental
public class ClusterManagementServiceBuilder
    extends BaseManagementServiceBuilder<ClusterManagementServiceBuilder> {

  private String host = System.getProperty("geode.config.cms.connection.hostname", "localhost");
  private int port = Integer.getInteger("geode.config.cms.connection.port", 7070);

  public ClusterManagementServiceBuilder setHost(String hostname) {
    host = hostname;
    return this;
  }

  public ClusterManagementServiceBuilder setPort(int port) {
    this.port = port;
    return this;
  }

  protected ConnectionConfig createConnectionConfig() {
    ConnectionConfig newConnectionConfig = new ConnectionConfig(host, port);
    return newConnectionConfig;
  }
}
