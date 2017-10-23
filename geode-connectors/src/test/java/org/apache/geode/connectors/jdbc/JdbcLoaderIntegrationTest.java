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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.connectors.jdbc.internal.SqlHandler;
import org.apache.geode.connectors.jdbc.internal.TestConfigService;
import org.apache.geode.connectors.jdbc.internal.TestableConnectionManager;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class JdbcLoaderIntegrationTest {

  private static final String DB_NAME = "DerbyDB";
  private static final String REGION_TABLE_NAME = "employees";
  private static final String CONNECTION_URL = "jdbc:derby:memory:" + DB_NAME + ";create=true";

  private Cache cache;
  private Connection connection;
  private Statement statement;

  @Before
  public void setup() throws Exception {
    cache = new CacheFactory().setPdxReadSerialized(false).create();
    connection = DriverManager.getConnection(CONNECTION_URL);
    statement = connection.createStatement();
    statement.execute("Create Table " + REGION_TABLE_NAME
        + " (id varchar(10) primary key not null, name varchar(10), age int)");
  }

  @After
  public void tearDown() throws Exception {
    cache.close();
    closeDB();
  }

  private void closeDB() throws Exception {
    if (statement == null) {
      statement = connection.createStatement();
    }
    statement.execute("Drop table " + REGION_TABLE_NAME);
    statement.close();

    if (connection != null) {
      connection.close();
    }
  }

  @Test
  public void verifySimpleGet() throws SQLException {
    statement.execute("Insert into " + REGION_TABLE_NAME + " values('1', 'Emp1', 21)");
    Region<String, PdxInstance> region = createRegionWithJDBCLoader(REGION_TABLE_NAME);
    PdxInstance pdx = region.get("1");

    assertThat(pdx.getField("name")).isEqualTo("Emp1");
    assertThat(pdx.getField("age")).isEqualTo(21);
  }

  @Test
  public void verifySimpleMiss() throws SQLException {
    Region<String, PdxInstance> region = createRegionWithJDBCLoader(REGION_TABLE_NAME);
    PdxInstance pdx = region.get("1");
    assertThat(pdx).isNull();
  }

  private SqlHandler createSqlHandler() {
    return new SqlHandler(new TestableConnectionManager(TestConfigService.getTestConfigService()));
  }

  private Region<String, PdxInstance> createRegionWithJDBCLoader(String regionName) {
    JdbcLoader<String, PdxInstance> jdbcLoader = new JdbcLoader<>(createSqlHandler());
    RegionFactory<String, PdxInstance> rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
    rf.setCacheLoader(jdbcLoader);
    return rf.create(regionName);
  }
}
