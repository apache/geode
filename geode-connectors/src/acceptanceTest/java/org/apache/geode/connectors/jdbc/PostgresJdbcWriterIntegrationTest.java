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

import static org.apache.geode.connectors.jdbc.test.junit.rules.SqlDatabaseConnectionRule.DEFAULT_DB_NAME;
import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.connectors.jdbc.test.junit.rules.DatabaseConnectionRule;
import org.apache.geode.connectors.jdbc.test.junit.rules.PostgresConnectionRule;

public class PostgresJdbcWriterIntegrationTest extends JdbcWriterIntegrationTest {

  private static final URL COMPOSE_RESOURCE_PATH =
      PostgresJdbcWriterIntegrationTest.class.getResource("/postgres.yml");

  @ClassRule
  public static DatabaseConnectionRule dbRule =
      new PostgresConnectionRule.Builder().file(COMPOSE_RESOURCE_PATH.getPath()).build();

  @Override
  public Connection getConnection() throws SQLException {
    return dbRule.getConnection();
  }

  @Override
  public Supplier<String> getConnectionUrlSupplier() {
    return () -> dbRule.getConnectionUrl();
  }

  @Override
  protected boolean vendorSupportsSchemas() {
    return true;
  }


  protected void createTableWithCatalogAndSchema() throws SQLException {
    statement.execute("Create Schema " + SCHEMA_NAME);
    statement
        .execute("Create Table " + DEFAULT_DB_NAME + '.' + SCHEMA_NAME + '.' + REGION_TABLE_NAME
            + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  @Test
  public void canDestroyFromTableWithCatalogAndSchema() throws Exception {
    createTableWithCatalogAndSchema();
    sharedRegionSetup("id", DEFAULT_DB_NAME, SCHEMA_NAME);
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    employees.destroy("1");

    ResultSet resultSet =
        statement.executeQuery("select * from " + DEFAULT_DB_NAME + '.' + SCHEMA_NAME + '.'
            + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canInsertIntoTableWithCatalogAndSchema() throws Exception {
    createTableWithCatalogAndSchema();
    sharedRegionSetup("id", DEFAULT_DB_NAME, SCHEMA_NAME);
    employees.put("1", pdx1);
    employees.put("2", pdx2);

    ResultSet resultSet =
        statement.executeQuery("select * from " + DEFAULT_DB_NAME + '.' + SCHEMA_NAME + '.'
            + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee1);
    assertRecordMatchesEmployee(resultSet, "2", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Test
  public void canUpdateTableWithCatalogAndSchema() throws Exception {
    createTableWithCatalogAndSchema();
    sharedRegionSetup("id", DEFAULT_DB_NAME, SCHEMA_NAME);
    employees.put("1", pdx1);
    employees.put("1", pdx2);

    ResultSet resultSet =
        statement.executeQuery("select * from " + DEFAULT_DB_NAME + '.' + SCHEMA_NAME + '.'
            + REGION_TABLE_NAME + " order by id asc");
    assertRecordMatchesEmployee(resultSet, "1", employee2);
    assertThat(resultSet.next()).isFalse();
  }

  @Override
  protected String getDataTooLongSqlErrorMessage() {
    return "ERROR: value too long for type character";
  }
}
