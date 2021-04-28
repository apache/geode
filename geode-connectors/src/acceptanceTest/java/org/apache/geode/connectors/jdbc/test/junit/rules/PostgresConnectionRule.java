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

public class PostgresConnectionRule extends SqlDatabaseConnectionRule {

  private static final int POSTGRES_PORT = 5432;

  private static final String CONNECTION_STRING = "jdbc:postgresql://%s:%d/%s?user=postgres";

  protected PostgresConnectionRule(String composeFile, String serviceName, int port,
      String dbName) {
    super(composeFile, serviceName, port, dbName);
  }

  @Override
  public String getConnectionUrl() {
    return String.format(CONNECTION_STRING, "localhost", getDockerPort(), getDbName());
  }

  public static class Builder extends SqlDatabaseConnectionRule.Builder {

    public Builder() {
      super(POSTGRES_PORT, DEFAULT_SERVICE_NAME, DEFAULT_DB_NAME);
    }

    @Override
    public PostgresConnectionRule build() {
      return new PostgresConnectionRule(getComposeFile(), getServiceName(), getPort(),
          getDbName());
    }
  }
}
