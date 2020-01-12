/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.connectors.jdbc;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;

import org.apache.geode.connectors.jdbc.internal.SqlHandler.DataSourceFactory;

public class TestDataSourceFactory implements DataSourceFactory {
  private final String url;
  private HikariDataSource dataSource;

  public TestDataSourceFactory(String url) {
    this.url = url;
  }

  @Override
  public DataSource getDataSource(String dataSourceName) {
    if (dataSource == null) {
      dataSource = new HikariDataSource();
      dataSource.setJdbcUrl(url);
    }
    return dataSource;
  }

  public void close() {
    if (dataSource != null) {
      dataSource.close();
    }
  }
}
