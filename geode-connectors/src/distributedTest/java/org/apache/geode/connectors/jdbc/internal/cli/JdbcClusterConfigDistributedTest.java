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
import static org.mockito.Mockito.mock;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.connectors.jdbc.internal.JdbcConnectorService;
import org.apache.geode.connectors.jdbc.internal.TableMetaDataView;
import org.apache.geode.connectors.jdbc.internal.configuration.ConnectorService;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.JDBCConnectorTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({DistributedTest.class, JDBCConnectorTest.class})
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

  @Test
  public void recreateCacheFromClusterConfig() throws Exception {
    gfsh.connectAndVerify(locator);
    gfsh.executeAndAssertThat("create jdbc-connection --name=connection --url=url")
        .statusIsSuccess();
    gfsh.executeAndAssertThat(
        "create jdbc-mapping --region=regionName --connection=connection --table=testTable --pdx-class-name=myPdxClass --value-contains-primary-key --field-mapping=field1:column1,field2:column2")
        .statusIsSuccess();

    server.invoke(() -> {
      JdbcConnectorService service =
          ClusterStartupRule.getCache().getService(JdbcConnectorService.class);
      validateRegionMapping(service.getMappingForRegion("regionName"));
    });

    server.stopMember(false);

    server = cluster.startServerVM(1, locator.getPort());
    server.invoke(() -> {
      JdbcConnectorService service =
          ClusterStartupRule.getCache().getService(JdbcConnectorService.class);
      assertThat(service.getConnectionConfig("connection")).isNotNull();
      validateRegionMapping(service.getMappingForRegion("regionName"));
    });
  }

  private static void validateRegionMapping(ConnectorService.RegionMapping regionMapping) {
    assertThat(regionMapping).isNotNull();
    assertThat(regionMapping.getRegionName()).isEqualTo("regionName");
    assertThat(regionMapping.getConnectionConfigName()).isEqualTo("connection");
    assertThat(regionMapping.getTableName()).isEqualTo("testTable");
    assertThat(regionMapping.getPdxClassName()).isEqualTo("myPdxClass");
    assertThat(regionMapping.isPrimaryKeyInValue()).isEqualTo(true);
    assertThat(regionMapping.getColumnNameForField("field1", mock(TableMetaDataView.class)))
        .isEqualTo("column1");
    assertThat(regionMapping.getColumnNameForField("field2", mock(TableMetaDataView.class)))
        .isEqualTo("column2");
  }

}
