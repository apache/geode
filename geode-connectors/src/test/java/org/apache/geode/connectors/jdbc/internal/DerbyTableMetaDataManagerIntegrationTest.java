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
package org.apache.geode.connectors.jdbc.internal;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Rule;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.DatabaseConnectionRule;
import org.apache.geode.test.junit.rules.InMemoryDerbyConnectionRule;

@Category(IntegrationTest.class)
public class DerbyTableMetaDataManagerIntegrationTest extends TableMetaDataManagerIntegrationTest {

  @Rule
  public DatabaseConnectionRule dbRule = new InMemoryDerbyConnectionRule(DB_NAME);

  @Override
  protected Connection getConnection() throws SQLException {
    return dbRule.getConnection();
  }

  @Override
  protected void createTable() throws SQLException {
    statement.execute("Create Table " + REGION_TABLE_NAME
        + " (\"id\" varchar(10) primary key not null, \"name\" varchar(10), \"age\" int)");
  }
}
