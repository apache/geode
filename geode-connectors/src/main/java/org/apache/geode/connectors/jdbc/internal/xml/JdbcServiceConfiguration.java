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
package org.apache.geode.connectors.jdbc.internal.xml;

import java.util.ArrayList;
import java.util.List;

import org.apache.geode.InternalGemFireException;
import org.apache.geode.cache.Cache;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfigExistsException;
import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMappingExistsException;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;

public class JdbcServiceConfiguration implements Extension<Cache> {

  private final List<ConnectorService.Connection> connections = new ArrayList<>();
  private final List<ConnectorService.RegionMapping> mappings = new ArrayList<>();

  void addConnectionConfig(ConnectorService.Connection config) {
    connections.add(config);
  }

  void addRegionMapping(ConnectorService.RegionMapping mapping) {
    mappings.add(mapping);
  }

  @Override
  public XmlGenerator<Cache> getXmlGenerator() {
    return null;
  }

  @Override
  public void beforeCreate(Extensible<Cache> source, Cache cache) {
    // nothing
  }

  @Override
  public void onCreate(Extensible<Cache> source, Extensible<Cache> target) {
    InternalCache internalCache = (InternalCache) target;
    JdbcConnectorService service = internalCache.getService(JdbcConnectorService.class);
    connections.forEach(connection -> createConnectionConfig(service, connection));
    mappings.forEach(mapping -> createRegionMapping(service, mapping));
  }

  private void createConnectionConfig(JdbcConnectorService service,
      ConnectorService.Connection connectionConfig) {
    try {
      service.createConnectionConfig(connectionConfig);
    } catch (ConnectionConfigExistsException e) {
      throw new InternalGemFireException(e);
    }
  }

  private void createRegionMapping(JdbcConnectorService service,
      ConnectorService.RegionMapping regionMapping) {
    try {
      service.createRegionMapping(regionMapping);
    } catch (RegionMappingExistsException e) {
      throw new InternalGemFireException(e);
    }
  }
}
