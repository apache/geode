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
package org.apache.geode.connectors.jdbc.test.junit.rules;

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.testcontainers.containers.DockerComposeContainer;

public abstract class SqlDatabaseConnectionRule extends ExternalResource
    implements DatabaseConnectionRule {

  public static final String DEFAULT_DB_NAME = "test";
  protected static final String DEFAULT_SERVICE_NAME = "db";
  private DockerComposeContainer<?> dbContainer;
  private final String composeFile;
  private final String serviceName;
  private final int port;
  private final String dbName;

  protected SqlDatabaseConnectionRule(String composeFile, String serviceName, int port,
      String dbName) {
    this.composeFile = composeFile;
    this.serviceName = serviceName;
    this.port = port;
    this.dbName = dbName;
  }

  @Override
  public Statement apply(Statement base, Description description) {
    Statement dbStatement = new Statement() {
      @Override
      public void evaluate() throws Throwable {

        dbContainer = new DockerComposeContainer<>("db", new File(composeFile))
            .withExposedService(serviceName, port);
        dbContainer.withLocalCompose(true);
        dbContainer.start();

        try {
          base.evaluate(); // run the test
        } finally {
          dbContainer.stop();
        }
      }
    };

    return dbStatement;
  }

  protected Integer getDockerPort() {
    return dbContainer.getServicePort(serviceName, port);
  }

  protected String getDbName() {
    return dbName;
  }

  @Override
  public Connection getConnection() throws SQLException {
    String connectionUrl = getConnectionUrl();
    await().ignoreExceptions()
        .untilAsserted(() -> assertThat(DriverManager.getConnection(connectionUrl)).isNotNull());
    Connection connection = DriverManager.getConnection(connectionUrl);
    return connection;
  }

  @Override
  public abstract String getConnectionUrl();

  public abstract static class Builder {
    private String filePath;
    private String serviceName;
    private int port;
    private String dbName;

    protected Builder(int port, String serviceName, String dbName) {
      this.port = port;
      this.serviceName = serviceName;
      this.dbName = dbName;
    }

    public abstract SqlDatabaseConnectionRule build();

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

    protected String getComposeFile() {
      return filePath;
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

  }

}
