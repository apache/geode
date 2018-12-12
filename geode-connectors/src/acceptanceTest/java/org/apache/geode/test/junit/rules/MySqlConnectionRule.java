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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.palantir.docker.compose.DockerComposeRule;

public class MySqlConnectionRule extends SqlDatabaseConnectionRule {
  private static final String CREATE_DB_CONNECTION_STRING =
      "jdbc:mysql://$HOST:$EXTERNAL_PORT?user=root&useSSL=false";

  private static final String CONNECTION_STRING =
      "jdbc:mysql://$HOST:$EXTERNAL_PORT/%s?user=root&useSSL=false";

  protected MySqlConnectionRule(DockerComposeRule dockerRule, String serviceName, int port,
      String dbName) {
    super(dockerRule, serviceName, port, dbName);
  }

  @Override
  public Connection getConnection() throws SQLException {
    await().ignoreExceptions()
        .untilAsserted(
            () -> assertThat(DriverManager.getConnection(getCreateDbConnectionUrl())).isNotNull());
    String dbName = getDbName();
    if (dbName != null) {
      Connection connection = DriverManager.getConnection(getCreateDbConnectionUrl());
      connection.createStatement().execute("CREATE DATABASE IF NOT EXISTS " + dbName);
    }
    return DriverManager.getConnection(getConnectionUrl());
  }

  @Override
  public String getConnectionUrl() {
    return getDockerPort().inFormat(String.format(CONNECTION_STRING, getDbName()));
  }


  public String getCreateDbConnectionUrl() {
    return getDockerPort().inFormat(CREATE_DB_CONNECTION_STRING);
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
