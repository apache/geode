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

import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;


public class DescribeJndiBindingCommandDUnitTest {

  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @BeforeClass
  public static void before() throws Exception {
    Properties props = new Properties();
    // props.setProperty(ConfigurationProperties.SERIALIZABLE_OBJECT_FILTER,
    // ListJndiBindingFunction.class.getName());

    MemberVM locator = cluster.startLocatorVM(0);
    @SuppressWarnings("unused")
    MemberVM server = cluster.startServerVM(1, props, locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  @SuppressWarnings("deprecation")
  public void describeJndiBindingForSimpleDataSource() {
    gfsh.executeAndAssertThat(
        "create jndi-binding --name=jndi-simple --type=SIMPLE --jdbc-driver-class=org.apache.derby.jdbc.EmbeddedDriver --connection-url=\"jdbc:derby:newDB;create=true\" --username=joe --datasource-config-properties={'name':'prop1','value':'value1','type':'java.lang.String'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("describe jndi-binding --name=jndi-simple").statusIsSuccess()
        .tableHasRowWithValues("Property", "Value", "jndi-name", "jndi-simple")
        .tableHasRowWithValues("Property", "Value", "type", "SimpleDataSource")
        .tableHasRowWithValues("Property", "Value", "jdbc-driver-class",
            "org.apache.derby.jdbc.EmbeddedDriver")
        .tableHasRowWithValues("Property", "Value", "user-name", "joe").tableHasRowWithValues(
            "Property", "Value", "connection-url", "jdbc:derby:newDB;create=true");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void describeJndiBindingForManagedDataSource() {
    gfsh.executeAndAssertThat(
        "create jndi-binding --name=jndi-managed --type=MANAGED --jdbc-driver-class=org.apache.derby.jdbc.EmbeddedDriver --connection-url=\"jdbc:derby:newDB;create=true\" --init-pool-size=1 --max-pool-size=10 --idle-timeout-seconds=2 --blocking-timeout-seconds=11 --login-timeout-seconds=7 --datasource-config-properties={'name':'prop1','value':'value1','type':'java.lang.String'} --managed-conn-factory-class="
            + ManagedConnectionFactoryForTesting.class.getName())
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("describe jndi-binding --name=jndi-managed").statusIsSuccess()
        .tableHasRowWithValues("Property", "Value", "jndi-name", "jndi-managed")
        .tableHasRowWithValues("Property", "Value", "type", "ManagedDataSource")
        .tableHasRowWithValues("Property", "Value", "jdbc-driver-class",
            "org.apache.derby.jdbc.EmbeddedDriver")
        .tableHasRowWithValues("Property", "Value", "user-name", "")
        .tableHasRowWithValues("Property", "Value", "connection-url",
            "jdbc:derby:newDB;create=true")
        .tableHasRowWithValues("Property", "Value", "managed-conn-factory-class",
            ManagedConnectionFactoryForTesting.class.getName())
        .tableHasRowWithValues("Property", "Value", "init-pool-size", "1")
        .tableHasRowWithValues("Property", "Value", "max-pool-size", "10")
        .tableHasRowWithValues("Property", "Value", "idle-timeout-seconds", "2")
        .tableHasRowWithValues("Property", "Value", "blocking-timeout-seconds", "11")
        .tableHasRowWithValues("Property", "Value", "login-timeout-seconds", "7")
        .tableHasRowWithValues("Property", "Value", "prop1", "value1");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void describeJndiBindingForPooledDataSource() {
    gfsh.executeAndAssertThat(
        "create jndi-binding --name=jndi-pooled --type=POOLED --jdbc-driver-class=org.apache.derby.jdbc.EmbeddedDriver --connection-url=\"jdbc:derby:newDB;create=true\" --conn-pooled-datasource-class=org.apache.geode.internal.jta.CacheJTAPooledDataSourceFactory --init-pool-size=1 --max-pool-size=10 --idle-timeout-seconds=2 --blocking-timeout-seconds=11 --login-timeout-seconds=7 --datasource-config-properties={'name':'prop1','value':'value1','type':'java.lang.String'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("describe jndi-binding --name=jndi-pooled").statusIsSuccess()
        .tableHasRowWithValues("Property", "Value", "jndi-name", "jndi-pooled")
        .tableHasRowWithValues("Property", "Value", "type", "PooledDataSource")
        .tableHasRowWithValues("Property", "Value", "jdbc-driver-class",
            "org.apache.derby.jdbc.EmbeddedDriver")
        .tableHasRowWithValues("Property", "Value", "user-name", "")
        .tableHasRowWithValues("Property", "Value", "connection-url",
            "jdbc:derby:newDB;create=true")
        .tableHasRowWithValues("Property", "Value", "conn-pooled-datasource-class",
            "org.apache.geode.internal.jta.CacheJTAPooledDataSourceFactory")
        .tableHasRowWithValues("Property", "Value", "init-pool-size", "1")
        .tableHasRowWithValues("Property", "Value", "max-pool-size", "10")
        .tableHasRowWithValues("Property", "Value", "idle-timeout-seconds", "2")
        .tableHasRowWithValues("Property", "Value", "blocking-timeout-seconds", "11")
        .tableHasRowWithValues("Property", "Value", "login-timeout-seconds", "7")
        .tableHasRowWithValues("Property", "Value", "prop1", "value1");
  }

  @Test
  @SuppressWarnings("deprecation")
  public void describeJndiBindingForXAPooledDataSource() {
    gfsh.executeAndAssertThat(
        "create jndi-binding --name=jndi-xapooled --type=XAPOOLED --jdbc-driver-class=org.apache.derby.jdbc.EmbeddedDriver --connection-url=\"jdbc:derby:newDB;create=true\" --xa-datasource-class=org.apache.derby.jdbc.EmbeddedXADataSource --init-pool-size=1 --max-pool-size=10 --idle-timeout-seconds=2 --blocking-timeout-seconds=11 --login-timeout-seconds=7 --datasource-config-properties={'name':'prop1','value':'value1','type':'java.lang.String'},{'name':'databaseName','value':'newDB','type':'java.lang.String'},{'name':'createDatabase','value':'create','type':'java.lang.String'}")
        .statusIsSuccess().tableHasColumnOnlyWithValues("Member", "server-1");

    gfsh.executeAndAssertThat("describe jndi-binding --name=jndi-xapooled").statusIsSuccess()
        .tableHasRowWithValues("Property", "Value", "jndi-name", "jndi-xapooled")
        .tableHasRowWithValues("Property", "Value", "type", "XAPooledDataSource")
        .tableHasRowWithValues("Property", "Value", "jdbc-driver-class",
            "org.apache.derby.jdbc.EmbeddedDriver")
        .tableHasRowWithValues("Property", "Value", "user-name", "")
        .tableHasRowWithValues("Property", "Value", "connection-url",
            "jdbc:derby:newDB;create=true")
        .tableHasRowWithValues("Property", "Value", "xa-datasource-class",
            "org.apache.derby.jdbc.EmbeddedXADataSource")
        .tableHasRowWithValues("Property", "Value", "init-pool-size", "1")
        .tableHasRowWithValues("Property", "Value", "max-pool-size", "10")
        .tableHasRowWithValues("Property", "Value", "idle-timeout-seconds", "2")
        .tableHasRowWithValues("Property", "Value", "blocking-timeout-seconds", "11")
        .tableHasRowWithValues("Property", "Value", "login-timeout-seconds", "7")
        .tableHasRowWithValues("Property", "Value", "prop1", "value1");
  }

  @Test
  public void describeJndiBindingDoesNotExist() {
    gfsh.executeAndAssertThat("describe jndi-binding --name=unknown").statusIsError()
        .containsOutput("JNDI binding : unknown not found");
  }
}
