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

import org.apache.geode.cache.Cache;
import org.apache.geode.connectors.jdbc.internal.ConnectionConfiguration;
import org.apache.geode.connectors.jdbc.internal.InternalJdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.RegionMapping;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.extension.Extension;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;

public class JdbcServiceConfiguration implements Extension<Cache> {

  private final List<ConnectionConfiguration> connections = new ArrayList<>();
  private final List<RegionMapping> mappings = new ArrayList<>();

  void addConnectionConfig(ConnectionConfiguration config) {
    connections.add(config);
  }

  void addRegionMapping(RegionMapping mapping) {
    mappings.add(mapping);
  }

  @Override
  public XmlGenerator<Cache> getXmlGenerator() {
    return new JdbcConnectorServiceXmlGenerator(connections, mappings);
  }

  @Override
  public void beforeCreate(Extensible<Cache> source, Cache cache) {
    // nothing
  }

  @Override
  public void onCreate(Extensible<Cache> source, Extensible<Cache> target) {
    InternalCache internalCache = (InternalCache) target;
    InternalJdbcConnectorService service =
        internalCache.getService(InternalJdbcConnectorService.class);
    connections.forEach(connection -> service.createConnectionConfig(connection));
    mappings.forEach(mapping -> service.addOrUpdateRegionMapping(mapping));
  }
}
