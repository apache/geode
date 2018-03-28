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

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.DatabaseConnectionRule;
import org.apache.geode.test.junit.rules.MySqlConnectionRule;
import org.apache.geode.test.junit.rules.PostgresConnectionRule;

@Category(IntegrationTest.class)
public class MySqlJdbcAsyncWriterIntegrationTest extends JdbcAsyncWriterIntegrationTest {

  @ClassRule
  public static DatabaseConnectionRule dbRule =
      new MySqlConnectionRule.Builder().file("src/test/resources/docker/mysql.yml")
          .serviceName("db").port(3306).database(DB_NAME).build();

  @Override
  public Connection getConnection() throws SQLException {
    return dbRule.getConnection();
  }

  @Override
  public String getConnectionUrl() {
    return dbRule.getConnectionUrl();
  }
}
