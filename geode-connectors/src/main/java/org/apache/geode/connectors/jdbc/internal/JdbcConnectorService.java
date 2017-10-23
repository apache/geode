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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.cache.Cache;
import org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlGenerator;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

public class JdbcConnectorService implements InternalJdbcConnectorService {

  private final Map<String, ConnectionConfiguration> connectionsByName = new ConcurrentHashMap<>();
  private final Map<String, RegionMapping> mappingsByRegion = new ConcurrentHashMap<>();
  private volatile InternalCache cache;
  private boolean registered;

  public ConnectionConfiguration getConnectionConfig(String connectionName) {
    return connectionsByName.get(connectionName);
  }

  public RegionMapping getMappingForRegion(String regionName) {
    return mappingsByRegion.get(regionName);
  }

  @Override
  public void addOrUpdateConnectionConfig(ConnectionConfiguration config) {
    registerAsExtension();
    connectionsByName.put(config.getName(), config);
  }

  @Override
  public void addOrUpdateRegionMapping(RegionMapping mapping) {
    registerAsExtension();
    mappingsByRegion.put(mapping.getRegionName(), mapping);
  }

  @Override
  public void init(Cache cache) {
    this.cache = (InternalCache) cache;
  }

  private synchronized void registerAsExtension() {
    if (!registered) {
      cache.getExtensionPoint().addExtension(this);
      registered = true;
    }
  }

  @Override
  public Class<? extends CacheService> getInterface() {
    return InternalJdbcConnectorService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }

  @Override
  public XmlGenerator<Cache> getXmlGenerator() {
    return new JdbcConnectorServiceXmlGenerator(connectionsByName.values(),
        mappingsByRegion.values());
  }

  @Override
  public void beforeCreate(Extensible<Cache> source, Cache cache) {
    // nothing
  }

  @Override
  public void onCreate(Extensible<Cache> source, Extensible<Cache> target) {
    // nothing
  }
}
