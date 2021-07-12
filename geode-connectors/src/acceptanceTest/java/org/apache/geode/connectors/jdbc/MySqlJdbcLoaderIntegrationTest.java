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
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import org.junit.ClassRule;

import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.test.junit.rules.DatabaseConnectionRule;
import org.apache.geode.connectors.jdbc.test.junit.rules.MySqlConnectionRule;
import org.apache.geode.pdx.FieldType;

public class MySqlJdbcLoaderIntegrationTest extends JdbcLoaderIntegrationTest {

  private static final URL COMPOSE_RESOURCE_PATH =
      MySqlJdbcLoaderIntegrationTest.class.getResource("/mysql.yml");

  @ClassRule
  public static DatabaseConnectionRule dbRule =
      new MySqlConnectionRule.Builder().file(COMPOSE_RESOURCE_PATH.getPath()).build();

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
        + "aboolean smallint, " + "abyte smallint, " + "ashort smallint, " + "anint int, "
        + "along bigint, " + "afloat float, " + "adouble float, " + "astring varchar(10), "
        + "adate datetime, " + "anobject varchar(20), " + "abytearray blob(100), "
        + "achar char(1))");
  }

  @Override
  protected List<FieldMapping> getSupportedPdxFieldsTableFieldMappings() {
    List<FieldMapping> fieldMappings = Arrays.asList(
        new FieldMapping("id", FieldType.STRING.name(), "id", JDBCType.VARCHAR.name(), false),
        new FieldMapping("aboolean", FieldType.BOOLEAN.name(), "aboolean", JDBCType.SMALLINT.name(),
            true),
        new FieldMapping("aByte", FieldType.BYTE.name(), "abyte", JDBCType.SMALLINT.name(), true),
        new FieldMapping("ASHORT", FieldType.SHORT.name(), "ashort", JDBCType.SMALLINT.name(),
            true),
        new FieldMapping("anint", FieldType.INT.name(), "anint", JDBCType.INTEGER.name(), true),
        new FieldMapping("along", FieldType.LONG.name(), "along", JDBCType.BIGINT.name(), true),
        new FieldMapping("afloat", FieldType.FLOAT.name(), "afloat", JDBCType.REAL.name(), true),
        new FieldMapping("adouble", FieldType.DOUBLE.name(), "adouble", JDBCType.REAL.name(),
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
    return false;
  }
}
