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
package org.apache.geode.connectors.jdbc.internal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;

import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.ExtensionPoint;

/**
 * Generates fake JdbcConnectorService with Connections and RegionMappings for tests.
 */
public class TestConfigService {
  private static final String DB_NAME = "DerbyDB";
  private static final String REGION_TABLE_NAME = "employees";
  private static final String REGION_NAME = "employees";
  private static final String CONNECTION_URL = "jdbc:derby:memory:" + DB_NAME + ";create=true";
  private static final String CONNECTION_CONFIG_NAME = "testConnectionConfig";

  public static JdbcConnectorService getTestConfigService() {
    InternalCache cache = mock(InternalCache.class);
    when(cache.getExtensionPoint()).thenReturn(mock(ExtensionPoint.class));

    JdbcConnectorService service = new JdbcConnectorService();
    service.init(cache);
    service.createConnectionConfig(createConnectionConfig());
    service.addOrUpdateRegionMapping(createRegionMapping());
    return service;
  }

  private static RegionMapping createRegionMapping() {
    return new RegionMapping(REGION_NAME, null, REGION_TABLE_NAME, CONNECTION_CONFIG_NAME, false,
        Collections.emptyMap());
  }

  private static ConnectionConfiguration createConnectionConfig() {
    return new ConnectionConfiguration(CONNECTION_CONFIG_NAME, CONNECTION_URL, null, null, null);
  }
}
