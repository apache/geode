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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.ClassRule;

import org.apache.geode.connectors.jdbc.test.junit.rules.PostgresConnectionRule;
import org.apache.geode.connectors.jdbc.test.junit.rules.SqlDatabaseConnectionRule;

/**
 * End-to-end dunits for jdbc connector
 */
public class PostgresJdbcDistributedTest extends JdbcDistributedTest {

  private static final URL COMPOSE_RESOURCE_PATH =
      PostgresJdbcDistributedTest.class.getResource("postgres.yml");

  @ClassRule
  public static transient SqlDatabaseConnectionRule dbRule = createConnectionRule();

  private static SqlDatabaseConnectionRule createConnectionRule() {
    try {
      return new PostgresConnectionRule.Builder().file(COMPOSE_RESOURCE_PATH.getPath())
          .serviceName("db").port(5432).database(DB_NAME).build();
    } catch (IllegalStateException e) {
      return null;
    }
  }

  @Override
  public Connection getConnection() throws SQLException {
    return dbRule.getConnection();
  }

  @Override
  public String getConnectionUrl() {
    return dbRule == null ? null : dbRule.getConnectionUrl();
  }

  @Override
  protected void createSupportedFieldsTable(Statement statement, String tableName, String quote)
      throws SQLException {
    statement.execute("CREATE TABLE " + quote + tableName + quote + " (" + quote + "id" + quote
        + " varchar(10) primary key not null, " + "aboolean boolean, " + "abyte smallint, "
        + "ashort smallint, " + "anint int, " + quote + "along" + quote + " bigint, " + quote
        + "aFloat" + quote + " float, " + quote + "ADOUBLE" + quote + " double precision, "
        + "astring varchar(10), " + "adate timestamp, " + "anobject varchar(20), "
        + "abytearray bytea, " + "achar char(1))");
  }

  @Override
  protected void createNullStatement(String key, PreparedStatement statement) throws SQLException {
    statement.setObject(1, key);
    statement.setNull(2, Types.BOOLEAN);
    statement.setNull(3, Types.SMALLINT);
    statement.setNull(4, Types.SMALLINT);
    statement.setNull(5, Types.INTEGER);
    statement.setNull(6, Types.BIGINT);
    statement.setNull(7, Types.FLOAT);
    statement.setNull(8, Types.DOUBLE);
    statement.setNull(9, Types.VARCHAR);
    statement.setNull(10, Types.TIMESTAMP);
    statement.setNull(11, Types.VARCHAR);
    statement.setNull(12, Types.ARRAY);
    statement.setNull(13, Types.CHAR);
  }
}
