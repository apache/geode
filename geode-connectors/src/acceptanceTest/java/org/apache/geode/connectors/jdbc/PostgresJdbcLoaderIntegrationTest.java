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
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.test.junit.rules.DatabaseConnectionRule;
import org.apache.geode.connectors.jdbc.test.junit.rules.PostgresConnectionRule;
import org.apache.geode.pdx.FieldType;
import org.apache.geode.pdx.PdxInstance;

public class PostgresJdbcLoaderIntegrationTest extends JdbcLoaderIntegrationTest {

  private static final URL COMPOSE_RESOURCE_PATH =
      PostgresJdbcLoaderIntegrationTest.class.getResource("/postgres.yml");

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
  protected void createClassWithSupportedPdxFieldsTable(Statement statement, String tableName)
      throws SQLException {
    statement.execute("CREATE TABLE " + tableName + " (id varchar(10) primary key not null, "
        + "aboolean boolean, " + "abyte smallint, " + "ashort smallint, " + "anint int, "
        + "along bigint, " + "afloat float, " + "adouble float, " + "astring varchar(10), "
        + "adate timestamp, " + "anobject varchar(20), " + "abytearray bytea, " + "achar char(1))");
  }

  @Override
  protected List<FieldMapping> getSupportedPdxFieldsTableFieldMappings() {
    List<FieldMapping> fieldMappings = Arrays.asList(
        new FieldMapping("id", FieldType.STRING.name(), "id", JDBCType.VARCHAR.name(), false),
        new FieldMapping("aboolean", FieldType.BOOLEAN.name(), "aboolean", JDBCType.BIT.name(),
            true),
        new FieldMapping("aByte", FieldType.BYTE.name(), "abyte", JDBCType.SMALLINT.name(), true),
        new FieldMapping("ASHORT", FieldType.SHORT.name(), "ashort", JDBCType.SMALLINT.name(),
            true),
        new FieldMapping("anint", FieldType.INT.name(), "anint", JDBCType.INTEGER.name(), true),
        new FieldMapping("along", FieldType.LONG.name(), "along", JDBCType.BIGINT.name(), true),
        new FieldMapping("afloat", FieldType.FLOAT.name(), "afloat", JDBCType.DOUBLE.name(), true),
        new FieldMapping("adouble", FieldType.DOUBLE.name(), "adouble", JDBCType.DOUBLE.name(),
            true),
        new FieldMapping("astring", FieldType.STRING.name(), "astring", JDBCType.VARCHAR.name(),
            true),
        new FieldMapping("adate", FieldType.DATE.name(), "adate", JDBCType.TIMESTAMP.name(), true),
        new FieldMapping("anobject", FieldType.OBJECT.name(), "anobject", JDBCType.VARCHAR.name(),
            true),
        new FieldMapping("abytearray", FieldType.BYTE_ARRAY.name(), "abytearray",
            JDBCType.BINARY.name(), true),
        new FieldMapping("achar", FieldType.CHAR.name(), "achar", JDBCType.CHAR.name(), true));
    return fieldMappings;
  }

  @Override
  protected boolean vendorSupportsSchemas() {
    return true;
  }

  private void createEmployeeTableWithSchemaAndCatalog() throws Exception {
    statement.execute("CREATE SCHEMA " + SCHEMA_NAME);
    statement
        .execute("Create Table " + DEFAULT_DB_NAME + '.' + SCHEMA_NAME + '.' + REGION_TABLE_NAME
            + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  @Test
  public void verifyGetWithSchemaCatalogAndPdxClassNameAndCompositeKey() throws Exception {
    createEmployeeTableWithSchemaAndCatalog();
    statement
        .execute("Insert into " + DEFAULT_DB_NAME + '.' + SCHEMA_NAME + '.' + REGION_TABLE_NAME
            + "(id, name, age) values('1', 'Emp1', 21)");
    String ids = "id,name";
    Region<String, Employee> region =
        createRegionWithJDBCLoader(REGION_TABLE_NAME, Employee.class.getName(), ids,
            DEFAULT_DB_NAME,
            SCHEMA_NAME, getEmployeeTableFieldMappings());
    createPdxType();

    PdxInstance key =
        cache.createPdxInstanceFactory("MyPdxKeyType").neverDeserialize().writeString("id", "1")
            .writeString("name", "Emp1").create();
    Employee value = region.get(key);

    assertThat(value.getId()).isEqualTo("1");
    assertThat(value.getName()).isEqualTo("Emp1");
    assertThat(value.getAge()).isEqualTo(21);
  }


}
