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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.json.JSONObject;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
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

  @Override
  protected boolean vendorSupportsSchemas() {
    return true;
  }
  
  private void createEmployeeTableWithSchemaAndCatalog() throws Exception {
    statement.execute("CREATE SCHEMA " + SCHEMA_NAME);
    statement.execute("Create Table " + DB_NAME + '.' + SCHEMA_NAME + '.' + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  @Test
  public void verifyGetWithSchemaCatalogAndPdxClassNameAndCompositeKey() throws Exception {
    createEmployeeTableWithSchemaAndCatalog();
    statement
        .execute("Insert into " + DB_NAME + '.' + SCHEMA_NAME + '.' + REGION_TABLE_NAME + "(id, name, age) values('1', 'Emp1', 21)");
    String ids = "id,name";
    Region<String, Employee> region =
        createRegionWithJDBCLoader(REGION_TABLE_NAME, Employee.class.getName(), ids, DB_NAME, SCHEMA_NAME);
    createPdxType();

    JSONObject key = new JSONObject();
    key.put("id", "1");
    key.put("name", "Emp1");
    Employee value = region.get(key.toString());

    assertThat(value.getId()).isEqualTo("1");
    assertThat(value.getName()).isEqualTo("Emp1");
    assertThat(value.getAge()).isEqualTo(21);
  }


}
