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

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.CacheCallback;
import org.apache.geode.cache.Operation;
import org.apache.geode.cache.Region;
import org.apache.geode.internal.cache.InternalCache;

@Experimental
public abstract class AbstractJdbcCallback implements CacheCallback {

  private volatile SqlHandler sqlHandler;
  protected InternalCache cache;

  protected AbstractJdbcCallback() {
    // nothing
  }

  protected AbstractJdbcCallback(SqlHandler sqlHandler, InternalCache cache) {
    this.sqlHandler = sqlHandler;
    this.cache = cache;
  }

  protected SqlHandler getSqlHandler() {
    return sqlHandler;
  }

  protected void checkInitialized(Region<?, ?> region) {
    if (sqlHandler == null) {
      initialize(region);
    }
  }

  protected boolean eventCanBeIgnored(Operation operation) {
    return operation.isLoad();
  }

  private synchronized void initialize(Region<?, ?> region) {
    if (sqlHandler == null) {
      cache = (InternalCache) region.getRegionService();
      JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
      TableMetaDataManager tableMetaDataManager = new TableMetaDataManager();
      sqlHandler = createSqlHandler(cache, region.getName(), tableMetaDataManager, service);
    }
  }

  SqlHandler createSqlHandler(InternalCache cache, String regionName,
      TableMetaDataManager tableMetaDataManager, JdbcConnectorService service) {
    return new SqlHandler(cache, regionName, tableMetaDataManager, service);
  }
}
