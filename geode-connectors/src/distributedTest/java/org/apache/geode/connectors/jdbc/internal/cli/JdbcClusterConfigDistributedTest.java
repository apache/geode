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
package org.apache.geode.connectors.jdbc.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import javax.sql.DataSource;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.configuration.FieldMapping;
import org.apache.geode.connectors.jdbc.internal.configuration.RegionMapping;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({JDBCConnectorTest.class})
@SuppressWarnings("serial")
public class JdbcClusterConfigDistributedTest {

  private static MemberVM locator, server;
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void beforeClass() {
    locator = cluster.startLocatorVM(0);
    server = cluster.startServerVM(1, locator.getPort());
  }

  private void setupDatabase() {
    gfsh.executeAndAssertThat(
        "create data-source --name=myDataSource"
            + " --pooled=false"
            + " --url=\"jdbc:derby:memory:newDB;create=true\"")
        .statusIsSuccess();
    executeSql(
        "create table mySchema.testTable (myId varchar(10) primary key, name varchar(10))");
  }

  private void teardownDatabase() {
    executeSql("drop table mySchema.testTable");
  }

  private void executeSql(String sql) {
    server.invoke(() -> {
      try {
        DataSource ds = JNDIInvoker.getDataSource("myDataSource");
        Connection conn = ds.getConnection();
        Statement sm = conn.createStatement();
        sm.execute(sql);
        sm.close();
        conn.close();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }


  public static class IdAndName implements PdxSerializable {
    private String id;
    private String name;

    public IdAndName() {
      // nothing
    }

    IdAndName(String id, String name) {
      this.id = id;
      this.name = name;
    }

    String getId() {
      return id;
    }

    String getName() {
      return name;
    }

    @Override
    public void toData(PdxWriter writer) {
      writer.writeString("myId", id);
      writer.writeString("name", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("myId");
      name = reader.readString("name");
    }
  }

  @Test
  public void recreateCacheFromClusterConfig() throws Exception {
    gfsh.connectAndVerify(locator);

    gfsh.executeAndAssertThat("create region --name=regionName --type=PARTITION").statusIsSuccess();
    setupDatabase();
    try {
      gfsh.executeAndAssertThat(
          "create jdbc-mapping --region=regionName --data-source=myDataSource --table=testTable --pdx-name="
              + IdAndName.class.getName() + " --schema=mySchema")
          .statusIsSuccess();

      server.invoke(() -> {
        JdbcConnectorService service =
            ClusterStartupRule.getCache().getService(JdbcConnectorService.class);
        validateRegionMapping(service.getMappingForRegion("regionName"));
      });

      server.stop(false);

      server = cluster.startServerVM(1, locator.getPort());
      server.invoke(() -> {
        JdbcConnectorService service =
            ClusterStartupRule.getCache().getService(JdbcConnectorService.class);
        validateRegionMapping(service.getMappingForRegion("regionName"));
      });
    } finally {
      teardownDatabase();
    }
  }

  private static void validateRegionMapping(RegionMapping regionMapping) {
    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo("regionName");
    assertThat(regionMapping.getDataSourceName()).isEqualTo("myDataSource");
    assertThat(regionMapping.getTableName()).isEqualTo("testTable");
    assertThat(regionMapping.getPdxName()).isEqualTo(IdAndName.class.getName());
    List<FieldMapping> fieldMappings = regionMapping.getFieldMappings();
    assertThat(fieldMappings).hasSize(2);
    assertThat(fieldMappings.get(0))
        .isEqualTo(new FieldMapping("myId", "STRING", "MYID", "VARCHAR", false));
    assertThat(fieldMappings.get(1))
        .isEqualTo(new FieldMapping("name", "STRING", "NAME", "VARCHAR", true));
  }

}
