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

package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.configuration.CacheConfig;
import org.apache.geode.cache.configuration.JndiBindingsType.JndiBinding;
import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.internal.logging.LogService;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableRunnableIF;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

public class CreateDataSourceCommandDUnitTest {

  private static MemberVM locator, server1, server2;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();


  @BeforeClass
  public static void before() throws Exception {
    locator = cluster.startLocatorVM(0);
    server1 = cluster.startServerVM(1, "group1", locator.getPort());
    server2 = cluster.startServerVM(2, "group1", locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testCreateDataSource() throws Exception {
    VMProvider.invokeInEveryMember(
        () -> assertThat(JNDIInvoker.getNoOfAvailableDataSources()).isEqualTo(0), server1, server2);

    String URL = "jdbc:derby:newDB;create=true";
    // create the data-source
    gfsh.executeAndAssertThat(
        "create data-source --name=jndi1 --username=myuser --password=mypass --pooled=false --url=\""
            + URL + "\"")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1", "server-2");

    // verify cluster config is updated
    locator.invoke(() -> {
      InternalConfigurationPersistenceService ccService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      CacheConfig cacheConfig = ccService.getCacheConfig("cluster");
      List<JndiBinding> jndiBindings = cacheConfig.getJndiBindings();
      assertThat(jndiBindings.size()).isEqualTo(1);
      JndiBinding jndiBinding = jndiBindings.get(0);
      assertThat(jndiBinding.getJndiName()).isEqualTo("jndi1");
      assertThat(jndiBinding.getUserName()).isEqualTo("myuser");
      assertThat(jndiBinding.getPassword()).isEqualTo("mypass");
      assertThat(jndiBinding.getConnectionUrl()).isEqualTo(URL);
      assertThat(jndiBinding.getType()).isEqualTo("SimpleDataSource");
    });

    // verify datasource exists
    VMProvider.invokeInEveryMember(
        () -> assertThat(JNDIInvoker.getNoOfAvailableDataSources()).isEqualTo(1), server1, server2);

    // bounce server1
    server1.stop(false);
    server1 = cluster.startServerVM(1, locator.getPort());

    // verify it has recreated the datasource from cluster config
    server1.invoke(() -> {
      assertThat(JNDIInvoker.getNoOfAvailableDataSources()).isEqualTo(1);
      assertThat(JNDIInvoker.getDataSource("jndi1")).isNotNull();
    });

    verifyThatNonExistentClassCausesGfshToError();
  }

  private void verifyThatNonExistentClassCausesGfshToError() {
    SerializableRunnableIF IgnoreClassNotFound = () -> {
      IgnoredException ex =
          new IgnoredException("non_existent_class_name");
      LogService.getLogger().info(ex.getAddMessage());
    };

    server1.invoke(IgnoreClassNotFound);
    server2.invoke(IgnoreClassNotFound);

    // create the binding
    gfsh.executeAndAssertThat(
        "create data-source --name=jndiBad --username=myuser --password=mypass --pooled --pooled-data-source-factory-class=non_existent_class_name --url=\"jdbc:derby:newDB;create=true\"")
        .statusIsError();
  }
}
