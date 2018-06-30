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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import org.apache.geode.distributed.internal.InternalConfigurationPersistenceService;
import org.apache.geode.internal.jndi.JNDIInvoker;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.VMProvider;

@Category({DistributedTest.class,})
public class CreateJndiBindingCommandDUnitTest {

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
  public void testCreateJndiBinding() throws Exception {
    // assert that is no datasource
    VMProvider.invokeInEveryMember(
        () -> assertThat(JNDIInvoker.getNoOfAvailableDataSources()).isEqualTo(0));

    // create the binding
    gfsh.executeAndAssertThat(
        "create jndi-binding --name=jndi1 --type=SIMPLE --jdbc-driver-class=org.apache.derby.jdbc.EmbeddedDriver --connection-url=\"jdbc:derby:newDB;create=true\"")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1", "server-2");

    // verify cluster config is updated
    locator.invoke(() -> {
      InternalConfigurationPersistenceService ccService =
          ClusterStartupRule.getLocator().getConfigurationPersistenceService();
      Configuration configuration = ccService.getConfiguration("cluster");
      Document document = XmlUtils.createDocumentFromXml(configuration.getCacheXmlContent());
      NodeList jndiBindings = document.getElementsByTagName("jndi-binding");

      assertThat(jndiBindings.getLength()).isGreaterThan(0);

      boolean found = false;
      for (int i = 0; i < jndiBindings.getLength(); i++) {
        Element eachBinding = (Element) jndiBindings.item(i);
        if (eachBinding.getAttribute("jndi-name").equals("jndi1")) {
          found = true;
          break;
        }
      }
      assertThat(found).isTrue();
    });

    // verify datasource exists
    VMProvider.invokeInEveryMember(
        () -> assertThat(JNDIInvoker.getNoOfAvailableDataSources()).isEqualTo(1));

    // bounce server1
    server1.stopMember(false);
    server1 = cluster.startServerVM(1, locator.getPort());

    // verify it has recreated the datasource from cluster config
    server1.invoke(() -> {
      assertThat(JNDIInvoker.getNoOfAvailableDataSources()).isEqualTo(1);
    });
  }
}
