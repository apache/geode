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

import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.ClassRule;

import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.connectors.jdbc.test.junit.rules.DatabaseConnectionRule;
import org.apache.geode.connectors.jdbc.test.junit.rules.MySqlConnectionRule;

public class MySqlTableMetaDataManagerIntegrationTest extends TableMetaDataManagerIntegrationTest {

  private static final URL COMPOSE_RESOURCE_PATH =
      MySqlTableMetaDataManagerIntegrationTest.class.getResource("/mysql.yml");

  @ClassRule
  public static DatabaseConnectionRule dbRule =
      new MySqlConnectionRule.Builder().file(COMPOSE_RESOURCE_PATH.getPath()).build();

  @Override
  public Connection getConnection() throws SQLException {
    return dbRule.getConnection();
  }

  @Override
  protected void setSchemaOrCatalogOnMapping(RegionMapping regionMapping, String name) {
    regionMapping.setCatalog(name);
  }

}
