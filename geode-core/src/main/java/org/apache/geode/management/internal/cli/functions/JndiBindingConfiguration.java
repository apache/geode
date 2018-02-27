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
package org.apache.geode.management.internal.cli.functions;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.geode.internal.datasource.ConfigProperty;

public class JndiBindingConfiguration implements Serializable {

  public enum DATASOURCE_TYPE {
    MANAGED("ManagedDataSource"),
    SIMPLE("SimpleDataSource"),
    POOLED("PooledDataSource"),
    XAPOOLED("XAPooledDataSource");

    private final String type;

    DATASOURCE_TYPE(String type) {
      this.type = type;
    }

    public String getType() {
      return this.type;
    }

    public String getName() {
      return name();
    }
  }

  private Integer blockingTimeout;
  private String connectionPoolDatasource;
  private String connectionUrl;
  private Integer idleTimeout;
  private Integer initPoolSize;
  private String jdbcDriver;
  private String jndiName;
  private Integer loginTimeout;
  private String managedConnFactory;
  private Integer maxPoolSize;
  private String password;
  private String transactionType;
  private DATASOURCE_TYPE type;
  private String username;
  private String xaDatasource;

  private List<ConfigProperty> datasourceConfigurations;

  public JndiBindingConfiguration() {
    datasourceConfigurations = new ArrayList<>();
  }

  public Integer getBlockingTimeout() {
    return blockingTimeout;
  }

  public void setBlockingTimeout(Integer blockingTimeout) {
    this.blockingTimeout = blockingTimeout;
  }

  public String getConnectionPoolDatasource() {
    return connectionPoolDatasource;
  }

  public void setConnectionPoolDatasource(String connectionPoolDatasource) {
    this.connectionPoolDatasource = connectionPoolDatasource;
  }

  public String getConnectionUrl() {
    return connectionUrl;
  }

  public void setConnectionUrl(String connectionUrl) {
    this.connectionUrl = connectionUrl;
  }

  public Integer getIdleTimeout() {
    return idleTimeout;
  }

  public void setIdleTimeout(Integer idleTimeout) {
    this.idleTimeout = idleTimeout;
  }

  public Integer getInitPoolSize() {
    return initPoolSize;
  }

  public void setInitPoolSize(Integer initPoolSize) {
    this.initPoolSize = initPoolSize;
  }

  public String getJdbcDriver() {
    return jdbcDriver;
  }

  public void setJdbcDriver(String jdbcDriver) {
    this.jdbcDriver = jdbcDriver;
  }

  public String getJndiName() {
    return jndiName;
  }

  public void setJndiName(String jndiName) {
    this.jndiName = jndiName;
  }

  public Integer getLoginTimeout() {
    return loginTimeout;
  }

  public void setLoginTimeout(Integer loginTimeout) {
    this.loginTimeout = loginTimeout;
  }

  public String getManagedConnFactory() {
    return managedConnFactory;
  }

  public void setManagedConnFactory(String managedConnFactory) {
    this.managedConnFactory = managedConnFactory;
  }

  public Integer getMaxPoolSize() {
    return maxPoolSize;
  }

  public void setMaxPoolSize(Integer maxPoolSize) {
    this.maxPoolSize = maxPoolSize;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public String getTransactionType() {
    return transactionType;
  }

  public void setTransactionType(String transactionType) {
    this.transactionType = transactionType;
  }

  public DATASOURCE_TYPE getType() {
    return type;
  }

  public void setType(DATASOURCE_TYPE type) {
    this.type = type;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getXaDatasource() {
    return xaDatasource;
  }

  public void setXaDatasource(String xaDatasource) {
    this.xaDatasource = xaDatasource;
  }

  public List<ConfigProperty> getDatasourceConfigurations() {
    return datasourceConfigurations;
  }

  public void setDatasourceConfigurations(List<ConfigProperty> dsConfigurations) {
    this.datasourceConfigurations = dsConfigurations;
  }

  public Map getParamsAsMap() {
    Map params = new HashMap();
    params.put("blocking-timeout-seconds", getBlockingTimeout());
    params.put("conn-pooled-datasource-class", getConnectionPoolDatasource());
    params.put("connection-url", getConnectionUrl());
    params.put("idle-timeout-seconds", getIdleTimeout());
    params.put("init-pool-size", getInitPoolSize());
    params.put("jdbc-driver-class", getJdbcDriver());
    params.put("jndi-name", getJndiName());
    params.put("login-timeout-seconds", getLoginTimeout());
    params.put("managed-conn-factory-class", getManagedConnFactory());
    params.put("max-pool-size", getMaxPoolSize());
    params.put("password", getPassword());
    params.put("transaction-type", getTransactionType());
    params.put("type", getType().getType());
    params.put("user-name", getUsername());
    params.put("xa-datasource-class", getXaDatasource());
    return params;
  }
}
