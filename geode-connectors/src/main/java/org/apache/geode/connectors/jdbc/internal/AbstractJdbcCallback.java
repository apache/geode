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

import java.util.Properties;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.CacheCallback;
import org.apache.geode.internal.cache.InternalCache;

@Experimental
public abstract class AbstractJdbcCallback implements CacheCallback {

  private volatile SqlHandler sqlHandler;
  protected volatile InternalCache cache;

  protected AbstractJdbcCallback() {
    // nothing
  }

  protected AbstractJdbcCallback(SqlHandler sqlHandler, InternalCache cache) {
    this.sqlHandler = sqlHandler;
    this.cache = cache;
  }

  @Override
  public void close() {
    if (sqlHandler != null) {
      sqlHandler.close();
    }
  }

  @Override
  public void init(Properties props) {
    // nothing
  }

  protected SqlHandler getSqlHandler() {
    return sqlHandler;
  }

  protected void checkInitialized(InternalCache cache) {
    if (sqlHandler == null) {
      initialize(cache);
    }
  }

  private synchronized void initialize(InternalCache cache) {
    if (sqlHandler == null) {
      this.cache = cache;
      JdbcConnectorService service = cache.getService(JdbcConnectorService.class);
      DataSourceManager manager = new DataSourceManager(new HikariJdbcDataSourceFactory());
      sqlHandler = new SqlHandler(manager, service);
    }
  }
}
