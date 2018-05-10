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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.Cache;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.internal.cache.CacheService;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.management.internal.beans.CacheServiceMBeanBase;

@Experimental
public class JdbcConnectorServiceImpl implements JdbcConnectorService {

  private final Map<String, ConnectorService.Connection> connectionsByName =
      new ConcurrentHashMap<>();
  private final Map<String, ConnectorService.RegionMapping> mappingsByRegion =
      new ConcurrentHashMap<>();
  private final DataSourceManager manager =
      new DataSourceManager(new HikariJdbcDataSourceFactory());
  private volatile InternalCache cache;
  private boolean registered;

  @Override
  public void createConnectionConfig(ConnectorService.Connection config)
      throws ConnectionConfigExistsException {
    ConnectorService.Connection existing = connectionsByName.putIfAbsent(config.getName(), config);
    if (existing != null) {
      throw new ConnectionConfigExistsException("Connection " + config.getName() + " exists");
    }
  }

  @Override
  public void replaceConnectionConfig(ConnectorService.Connection alteredConfig)
      throws ConnectionConfigNotFoundException {
    ConnectorService.Connection existingConfig = connectionsByName.get(alteredConfig.getName());
    if (existingConfig == null) {
      throw new ConnectionConfigNotFoundException(
          "Connection configuration " + alteredConfig.getName() + " was not found");
    }

    connectionsByName.put(existingConfig.getName(), alteredConfig);
  }

  @Override
  public void destroyConnectionConfig(String connectionName) {
    connectionsByName.remove(connectionName);
  }

  @Override
  public ConnectorService.Connection getConnectionConfig(String connectionName) {
    return connectionsByName.get(connectionName);
  }

  @Override
  public Set<ConnectorService.Connection> getConnectionConfigs() {
    Set<ConnectorService.Connection> connectionConfigs = new HashSet<>();
    connectionConfigs.addAll(connectionsByName.values());
    return connectionConfigs;
  }

  @Override
  public Set<ConnectorService.RegionMapping> getRegionMappings() {
    Set<ConnectorService.RegionMapping> regionMappings = new HashSet<>();
    regionMappings.addAll(mappingsByRegion.values());
    return regionMappings;
  }

  @Override
  public DataSourceManager getDataSourceManager() {
    return manager;
  }

  @Override
  public void createRegionMapping(ConnectorService.RegionMapping mapping)
      throws RegionMappingExistsException {
    ConnectorService.RegionMapping existing =
        mappingsByRegion.putIfAbsent(mapping.getRegionName(), mapping);
    if (existing != null) {
      throw new RegionMappingExistsException(
          "RegionMapping for region " + mapping.getRegionName() + " exists");
    }
  }

  @Override
  public void replaceRegionMapping(ConnectorService.RegionMapping alteredMapping)
      throws RegionMappingNotFoundException {
    ConnectorService.RegionMapping existingMapping =
        mappingsByRegion.get(alteredMapping.getRegionName());
    if (existingMapping == null) {
      throw new RegionMappingNotFoundException(
          "RegionMapping for region " + existingMapping.getRegionName() + " was not found");
    }

    mappingsByRegion.put(existingMapping.getRegionName(), alteredMapping);
  }

  @Override
  public ConnectorService.RegionMapping getMappingForRegion(String regionName) {
    return mappingsByRegion.get(regionName);
  }

  @Override
  public void destroyRegionMapping(String regionName) {
    mappingsByRegion.remove(regionName);
  }

  @Override
  public void init(Cache cache) {
    this.cache = (InternalCache) cache;
  }


  @Override
  public Class<? extends CacheService> getInterface() {
    return JdbcConnectorService.class;
  }

  @Override
  public CacheServiceMBeanBase getMBean() {
    return null;
  }
}
