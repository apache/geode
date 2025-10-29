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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import javax.sql.DataSource;

import org.assertj.core.api.AssertionsForClassTypes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.pdx.PdxReader;
import org.apache.geode.pdx.PdxSerializable;
import org.apache.geode.pdx.PdxWriter;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

public class DestroyDataSourceCommandDUnitTest {
  private MemberVM locator, server1, server2;

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Before
  public void before() throws Exception {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startServerVM(1, locator.getPort());
    server2 = cluster.startServerVM(2, locator.getPort());

    gfsh.connectAndVerify(locator);

    gfsh.execute(
        "create data-source --name=datasource1 --url=\"jdbc:derby:memory:newDB;create=true\"");
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDestroyDataSource() {
    // assert that there is a datasource
    VMProvider
        .invokeInEveryMember(
            () -> assertThat(JNDIInvoker.getBindingNamesRecursively(JNDIInvoker.getJNDIContext()))
                .containsKey("java:datasource1").containsValue(
                    "com.zaxxer.hikari.HikariDataSource"),
            server1, server2);

    gfsh.executeAndAssertThat("destroy data-source --name=datasource1").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server-1", "server-2")
        .tableHasColumnOnlyWithValues("Message",
            "Data source \"datasource1\" destroyed on \"server-1\"",
            "Data source \"datasource1\" destroyed on \"server-2\"");

    // verify cluster config is updated
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      AssertionsForClassTypes.assertThat(internalLocator).isNotNull();
      InternalConfigurationPersistenceService ccService =
          internalLocator.getConfigurationPersistenceService();
      Configuration configuration = ccService.getConfiguration("cluster");
      Document document = XmlUtils.createDocumentFromXml(configuration.getCacheXmlContent());
      NodeList jndiBindings = document.getElementsByTagName("jndi-binding");

      AssertionsForClassTypes.assertThat(jndiBindings.getLength()).isEqualTo(0);

      boolean found = false;
      for (int i = 0; i < jndiBindings.getLength(); i++) {
        Element eachBinding = (Element) jndiBindings.item(i);
        if (eachBinding.getAttribute("jndi-name").equals("datasource1")) {
          found = true;
          break;
        }
      }
      AssertionsForClassTypes.assertThat(found).isFalse();
    });

    // verify datasource does not exists
    VMProvider.invokeInEveryMember(
        () -> AssertionsForClassTypes.assertThat(JNDIInvoker.getNoOfAvailableDataSources())
            .isEqualTo(0),
        server1, server2);

    // bounce server1 and assert that there is still no datasource received from cluster config
    server1.stop(false);
    server1 = cluster.startServerVM(1, locator.getPort());

    // verify no datasource from cluster config
    server1.invoke(() -> {
      AssertionsForClassTypes.assertThat(JNDIInvoker.getNoOfAvailableDataSources()).isEqualTo(0);
    });
  }

  @SuppressWarnings("deprecation")
  @Test
  public void testDestroySimpleDataSource() throws Exception {
    // drop the default pooled data source
    gfsh.executeAndAssertThat("destroy data-source --name=datasource1").statusIsSuccess();

    // create a simple data source, i.e. non-pooled data source
    gfsh.execute(
        "create data-source --name=datasource2 --url=\"jdbc:derby:memory:newDB;create=true\" --pooled=false");

    // assert that there is a datasource
    VMProvider
        .invokeInEveryMember(
            () -> assertThat(JNDIInvoker.getBindingNamesRecursively(JNDIInvoker.getJNDIContext()))
                .containsKey("java:datasource2").containsValue(
                    "org.apache.geode.internal.datasource.GemFireBasicDataSource"),
            server1, server2);


    gfsh.executeAndAssertThat("destroy data-source --name=datasource2").statusIsSuccess()
        .tableHasColumnOnlyWithValues("Member", "server-1", "server-2")
        .tableHasColumnOnlyWithValues("Message",
            "Data source \"datasource2\" destroyed on \"server-1\"",
            "Data source \"datasource2\" destroyed on \"server-2\"");

    // verify cluster config is updated
    locator.invoke(() -> {
      InternalLocator internalLocator = ClusterStartupRule.getLocator();
      AssertionsForClassTypes.assertThat(internalLocator).isNotNull();
      InternalConfigurationPersistenceService ccService =
          internalLocator.getConfigurationPersistenceService();
      Configuration configuration = ccService.getConfiguration("cluster");
      Document document = XmlUtils.createDocumentFromXml(configuration.getCacheXmlContent());
      NodeList jndiBindings = document.getElementsByTagName("jndi-binding");

      AssertionsForClassTypes.assertThat(jndiBindings.getLength()).isEqualTo(0);

      boolean found = false;
      for (int i = 0; i < jndiBindings.getLength(); i++) {
        Element eachBinding = (Element) jndiBindings.item(i);
        if (eachBinding.getAttribute("jndi-name").equals("datasource2")) {
          found = true;
          break;
        }
      }
      AssertionsForClassTypes.assertThat(found).isFalse();
    });

    // verify datasource does not exists
    VMProvider.invokeInEveryMember(
        () -> AssertionsForClassTypes.assertThat(JNDIInvoker.getNoOfAvailableDataSources())
            .isEqualTo(0),
        server1, server2);

    // bounce server1 and assert that there is still no datasource received from cluster config
    server1.stop(false);
    server1 = cluster.startServerVM(1, locator.getPort());

    // verify no datasource from cluster config
    server1.invoke(() -> {
      AssertionsForClassTypes.assertThat(JNDIInvoker.getNoOfAvailableDataSources()).isEqualTo(0);
    });
  }

  private void createTable() {
    executeSql("create table mySchema.myRegion (id varchar(10) primary key, name varchar(10))");
  }

  private void dropTable() {
    executeSql("drop table mySchema.myRegion");
  }

  private void executeSql(String sql) {
    for (MemberVM server : Arrays.asList(server1, server2)) {
      server.invoke(() -> {
        try {
          DataSource ds = JNDIInvoker.getDataSource("datasource1");
          Connection conn = ds.getConnection();
          Statement sm = conn.createStatement();
          sm.execute(sql);
          sm.close();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
    }
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
      writer.writeString("id", id);
      writer.writeString("name", name);
    }

    @Override
    public void fromData(PdxReader reader) {
      id = reader.readString("id");
      name = reader.readString("name");
    }
  }

  @Test
  public void destroyDataSourceFailsIfInUseByJdbcMapping() {
    gfsh.executeAndAssertThat("create region --name=myRegion --type=REPLICATE").statusIsSuccess();
    createTable();
    try {
      gfsh.executeAndAssertThat(
          "create jdbc-mapping --data-source=datasource1 --pdx-name=" + IdAndName.class.getName()
              + " --region=myRegion --schema=mySchema")
          .statusIsSuccess();

      gfsh.executeAndAssertThat("destroy data-source --name=datasource1").statusIsError()
          .containsOutput(
              "Data source named \"datasource1\" is still being used by region \"myRegion\"."
                  + " Use destroy jdbc-mapping --region=myRegion and then try again.");
    } finally {
      dropTable();
    }
  }

  @Test
  public void destroySimpleDataSourceFailsIfInUseByJdbcMapping() throws Exception {
    // create a simple data source, i.e. non-pooled data source
    gfsh.execute(
        "create data-source --name=datasource2 --url=\"jdbc:derby:memory:newDB;create=true\" --pooled=false");

    gfsh.executeAndAssertThat("create region --name=myRegion --type=REPLICATE").statusIsSuccess();
    createTable();
    try {
      gfsh.executeAndAssertThat(
          "create jdbc-mapping --data-source=datasource2 --pdx-name=" + IdAndName.class.getName()
              + " --region=myRegion --schema=mySchema")
          .statusIsSuccess();

      gfsh.executeAndAssertThat("destroy data-source --name=datasource2").statusIsError()
          .containsOutput(
              "Data source named \"datasource2\" is still being used by region \"myRegion\"."
                  + " Use destroy jdbc-mapping --region=myRegion and then try again.");
    } finally {
      dropTable();
    }
  }
}
