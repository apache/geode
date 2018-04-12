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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.geode.cache.Cache;
import org.apache.geode.connectors.jdbc.internal.xml.JdbcConnectorServiceXmlGenerator;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.extension.Extensible;
import org.apache.geode.internal.cache.xmlcache.XmlGenerator;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;


public class JdbcConnectorServiceImpl implements JdbcConnectorService {

  private final Map<String, ConnectionConfiguration> connectionsByName = new ConcurrentHashMap<>();
  private final Map<String, RegionMapping> mappingsByRegion = new ConcurrentHashMap<>();
  private final DataSourceManager manager =
      new DataSourceManager(new HikariJdbcDataSourceFactory());
  private volatile InternalCache cache;
  private boolean registered;

  @Override
  public void createConnectionConfig(ConnectionConfiguration config)
      throws ConnectionConfigExistsException {
    registerAsExtension();
    ConnectionConfiguration existing = connectionsByName.putIfAbsent(config.getName(), config);
    if (existing != null) {
      throw new ConnectionConfigExistsException(
          "ConnectionConfiguration " + config.getName() + " exists");
    }
  }

  @Override
  public void replaceConnectionConfig(ConnectionConfiguration alteredConfig)
      throws ConnectionConfigNotFoundException {
    registerAsExtension();
    ConnectionConfiguration existingConfig = connectionsByName.get(alteredConfig.getName());
    if (existingConfig == null) {
      throw new ConnectionConfigNotFoundException(
          "ConnectionConfiguration " + alteredConfig.getName() + " was not found");
    }

    connectionsByName.put(existingConfig.getName(), alteredConfig);
  }

  @Override
  public void destroyConnectionConfig(String connectionName) {
    registerAsExtension();
    connectionsByName.remove(connectionName);
  }

  @Override
  public ConnectionConfiguration getConnectionConfig(String connectionName) {
    return connectionsByName.get(connectionName);
  }

  @Override
  public Set<ConnectionConfiguration> getConnectionConfigs() {
    Set<ConnectionConfiguration> connectionConfigs = new HashSet<>();
    connectionConfigs.addAll(connectionsByName.values());
    return connectionConfigs;
  }

  @Override
  public Set<RegionMapping> getRegionMappings() {
    Set<RegionMapping> regionMappings = new HashSet<>();
    regionMappings.addAll(mappingsByRegion.values());
    return regionMappings;
  }

  @Override
  public DataSourceManager getDataSourceManager() {
    return manager;
  }

  @Override
  public void createRegionMapping(RegionMapping mapping) throws RegionMappingExistsException {
    registerAsExtension();
    RegionMapping existing = mappingsByRegion.putIfAbsent(mapping.getRegionName(), mapping);
    if (existing != null) {
      throw new RegionMappingExistsException(
          "RegionMapping for region " + mapping.getRegionName() + " exists");
    }
  }

  @Override
  public void replaceRegionMapping(RegionMapping alteredMapping)
      throws RegionMappingNotFoundException {
    registerAsExtension();
    RegionMapping existingMapping = mappingsByRegion.get(alteredMapping.getRegionName());
    if (existingMapping == null) {
      throw new RegionMappingNotFoundException(
          "RegionMapping for region " + existingMapping.getRegionName() + " was not found");
    }

    mappingsByRegion.put(existingMapping.getRegionName(), alteredMapping);
  }

  @Override
  public RegionMapping getMappingForRegion(String regionName) {
    return mappingsByRegion.get(regionName);
  }

  @Override
  public void destroyRegionMapping(String regionName) {
    registerAsExtension();
    mappingsByRegion.remove(regionName);
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
    return JdbcConnectorService.class;
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
