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

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.management.internal.cli.functions.ListJndiBindingFunction;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.assertions.CommandResultAssert;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class ListJndiBindingCommandDUnitTest {

  private static MemberVM locator, server;

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
        ListJndiBindingFunction.class.getName());

    locator = cluster.startLocatorVM(0);
    server = cluster.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void listJndiBinding() {
    gfsh.executeAndAssertThat(
        "create jndi-binding --name=jndi1 --type=SIMPLE --jdbc-driver-class=org.apache.derby.jdbc.EmbeddedDriver --connection-url=\"jdbc:derby:newDB;create=true\"")
        .statusIsSuccess()
        .hasTableSection()
        .hasColumn("Member").containsExactly("server-1");

    CommandResultAssert commandResultAssert =
        gfsh.executeAndAssertThat("list jndi-binding").statusIsSuccess();
    commandResultAssert
        .hasTableSection("clusterConfiguration")
        .hasRowSize(1)
        .hasRow(0)
        .containsExactly("cluster", "jndi1", "org.apache.derby.jdbc.EmbeddedDriver");

    // member table
    commandResultAssert.hasTableSection("memberConfiguration")
        .hasRowSize(3)
        .hasColumn("JNDI Name").containsExactlyInAnyOrder("java:jndi1", "java:UserTransaction",
            "java:TransactionManager");
  }
}
