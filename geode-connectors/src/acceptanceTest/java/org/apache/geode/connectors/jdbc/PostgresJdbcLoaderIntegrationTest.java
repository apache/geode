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

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.ClassRule;

import org.apache.geode.test.junit.rules.DatabaseConnectionRule;
import org.apache.geode.test.junit.rules.PostgresConnectionRule;

public class PostgresJdbcLoaderIntegrationTest extends JdbcLoaderIntegrationTest {

  private static final URL COMPOSE_RESOURCE_PATH =
      PostgresJdbcLoaderIntegrationTest.class.getResource("postgres.yml");

  @ClassRule
  public static DatabaseConnectionRule dbRule = new PostgresConnectionRule.Builder()
      .file(COMPOSE_RESOURCE_PATH.getPath()).serviceName("db").port(5432).database(DB_NAME).build();

  @Override
  public Connection getConnection() throws SQLException {
    return dbRule.getConnection();
  }

  @Override
  public String getConnectionUrl() {
    return dbRule.getConnectionUrl();
  }

  @Override
  protected void createClassWithSupportedPdxFieldsTable(Statement statement, String tableName)
      throws SQLException {
    statement.execute("CREATE TABLE " + tableName + " (id varchar(10) primary key not null, "
        + "aboolean boolean, " + "abyte smallint, " + "ashort smallint, " + "anint int, "
        + "along bigint, " + "afloat float, " + "adouble float, " + "astring varchar(10), "
        + "adate timestamp, " + "anobject varchar(20), " + "abytearray bytea, " + "achar char(1))");
  }
}
