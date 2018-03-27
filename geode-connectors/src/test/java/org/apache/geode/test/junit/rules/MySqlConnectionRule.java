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

import java.sql.Connection;
import java.sql.SQLException;

import com.palantir.docker.compose.DockerComposeRule;

public class MySqlConnectionRule extends SqlDatabaseConnectionRule {
  private static final String CONNECTION_STRING =
      "jdbc:mysql://$HOST:$EXTERNAL_PORT?user=root&useSSL=false";

  protected MySqlConnectionRule(DockerComposeRule dockerRule, String serviceName, int port,
      String dbName) {
    super(dockerRule, serviceName, port, dbName);
  }

  @Override
  public Connection getConnection() throws SQLException {
    Connection connection = super.getConnection();
    String dbName = getDbName();
    if (dbName != null) {
      connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS " + dbName);
      connection.setCatalog(dbName);
    }
    return connection;
  }

  @Override
  protected String getConnectionString() {
    return getDockerPort().inFormat(CONNECTION_STRING);
  }

  public static class Builder extends SqlDatabaseConnectionRule.Builder {

    public Builder() {
      super();
    }

    @Override
    public MySqlConnectionRule build() {
      return new MySqlConnectionRule(createDockerRule(), getServiceName(), getPort(), getDbName());
    }
  }
}
