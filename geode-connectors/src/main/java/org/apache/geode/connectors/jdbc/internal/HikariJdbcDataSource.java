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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;

import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.internal.datasource.ConfiguredDataSourceProperties;

public class HikariJdbcDataSource implements DataSource, JdbcDataSource {

  private final HikariDataSource delegate;

  HikariJdbcDataSource(ConnectorService.Connection config) {
    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl(config.getUrl());
    ds.setUsername(config.getUser());
    ds.setPassword(config.getPassword());
    ds.setDataSourceProperties(config.getConnectionProperties());
    this.delegate = ds;
  }

  // TODO: set more config properties
  public HikariJdbcDataSource(ConfiguredDataSourceProperties config) {
    HikariDataSource ds = new HikariDataSource();
    ds.setJdbcUrl(config.getURL());
    ds.setUsername(config.getUser());
    ds.setPassword(config.getPassword());
    // ds.setDataSourceProperties(config.getConnectionProperties());
    this.delegate = ds;
  }

  @Override
  public Connection getConnection() throws SQLException {
    return this.delegate.getConnection();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    return this.delegate.getConnection(username, password);
  }

  @Override
  public void close() {
    this.delegate.close();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return this.delegate.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return this.delegate.isWrapperFor(iface);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return this.delegate.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    this.delegate.setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    this.delegate.setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return this.delegate.getLoginTimeout();
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return this.delegate.getParentLogger();
  }
}
