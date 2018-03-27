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
package org.apache.geode.test.junit.rules;

import static org.awaitility.Awaitility.matches;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.connection.DockerPort;
import com.palantir.docker.compose.connection.waiting.HealthChecks;
import org.awaitility.Awaitility;
import org.junit.rules.ExternalResource;

public abstract class DatabaseConnectionRule extends ExternalResource {

  private final DockerComposeRule dockerRule;
  private final String serviceName;
  private final int port;
  private final String dbName;

  protected DatabaseConnectionRule(DockerComposeRule dockerRule, String serviceName, int port,
      String dbName) {
    this.dockerRule = dockerRule;
    this.serviceName = serviceName;
    this.port = port;
    this.dbName = dbName;
  }

  @Override
  public void before() throws IOException, InterruptedException {
    dockerRule.before();
  }

  @Override
  public void after() {
    dockerRule.after();
  }

  protected DockerPort getDockerPort() {
    return dockerRule.containers().container(serviceName).port(port);
  }

  protected String getDbName() {
    return dbName;
  }

  public Connection getConnection() throws SQLException {
    String connectionUrl = getConnectionString();
    Awaitility.await().ignoreExceptions().atMost(10, TimeUnit.SECONDS)
        .until(matches(() -> DriverManager.getConnection(connectionUrl)));
    Connection connection = DriverManager.getConnection(connectionUrl);
    return connection;
  }

  protected abstract String getConnectionString();

  public abstract static class Builder {
    private String filePath;
    private String serviceName;
    private int port;
    private String dbName;

    public abstract DatabaseConnectionRule build();

    public Builder file(String filePath) {
      this.filePath = filePath;
      return this;
    }

    public Builder serviceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder database(String dbName) {
      this.dbName = dbName;
      return this;
    }

    protected String getDbName() {
      return dbName;
    }

    protected String getServiceName() {
      return serviceName;
    }

    protected int getPort() {
      return port;
    }

    protected DockerComposeRule createDockerRule() {
      return DockerComposeRule.builder().file(filePath)
          .waitingForService(serviceName, HealthChecks.toHaveAllPortsOpen()).build();
    }

  }

}
