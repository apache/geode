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

import java.util.List;

import org.apache.geode.cache.Cache;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.ExtensionPoint;

/**
 * Generates fake JdbcConnectorService for tests.
 */
public class TestConfigService {
  private static final String REGION_TABLE_NAME = "employees";
  private static final String REGION_NAME = "employees";
  private static final String CONNECTION_CONFIG_NAME = "testConnectionConfig";

  public static JdbcConnectorServiceImpl getTestConfigService(String ids,
      List<FieldMapping> fieldMappings)
      throws RegionMappingExistsException {
    return getTestConfigService(createMockCache(), null, ids, null, null, fieldMappings);
  }

  public static JdbcConnectorServiceImpl getTestConfigService(InternalCache cache,
      String pdxClassName, String ids, String catalog, String schema,
      List<FieldMapping> fieldMappings)
      throws RegionMappingExistsException {

    JdbcConnectorServiceImpl service = new JdbcConnectorServiceImpl();
    service.init(cache);
    service.createRegionMapping(
        createRegionMapping(pdxClassName, ids, catalog, schema, fieldMappings));
    return service;
  }

  private static InternalCache createMockCache() {
    InternalCache cache = mock(InternalCache.class);
    @SuppressWarnings("unchecked")
    final ExtensionPoint<Cache> mockExtensionPoint = mock(ExtensionPoint.class);
    when(cache.getExtensionPoint()).thenReturn(mockExtensionPoint);
    return cache;
  }

  private static RegionMapping createRegionMapping(String pdxClassName, String ids, String catalog,
      String schema, List<FieldMapping> fieldMappings) {
    RegionMapping result = new RegionMapping(REGION_NAME, pdxClassName, REGION_TABLE_NAME,
        CONNECTION_CONFIG_NAME, ids, catalog, schema);
    for (FieldMapping fieldMapping : fieldMappings) {
      result.addFieldMapping(fieldMapping);
    }
    return result;
  }
}
