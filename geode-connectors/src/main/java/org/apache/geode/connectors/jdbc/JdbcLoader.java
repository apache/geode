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
package org.apache.geode.connectors.jdbc;

import java.sql.SQLException;

import org.apache.geode.annotations.Experimental;
import org.apache.geode.cache.CacheLoader;
import org.apache.geode.cache.CacheLoaderException;
import org.apache.geode.cache.LoaderHelper;
import org.apache.geode.connectors.jdbc.internal.AbstractJdbcCallback;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.internal.cache.InternalCache;

/**
 * This class provides loading from a data source using JDBC.
 *
 * @since Geode 1.4
 */
@Experimental
public class JdbcLoader<K, V> extends AbstractJdbcCallback implements CacheLoader<K, V> {

  @SuppressWarnings("unused")
  public JdbcLoader() {
    super();
  }

  // Constructor for test purposes only
  JdbcLoader(SqlHandler sqlHandler, InternalCache cache) {
    super(sqlHandler, cache);
  }

  /**
   * @return this method always returns a PdxInstance. It does not matter what the V generic
   *         parameter is set to.
   */
  @Override
  @SuppressWarnings("unchecked")
  public V load(LoaderHelper<K, V> helper) throws CacheLoaderException {
    checkInitialized(helper.getRegion());
    try {
      // The following cast to V is to keep the compiler happy
      // but is erased at runtime and no actual cast happens.
      return (V) getSqlHandler().read(helper.getRegion(), helper.getKey());
    } catch (SQLException e) {
      throw JdbcConnectorException.createException(e);
    }
  }
}
