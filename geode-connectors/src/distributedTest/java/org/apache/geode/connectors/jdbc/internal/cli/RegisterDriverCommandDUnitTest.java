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

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.File;

import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.util.test.TestUtil;

public class RegisterDriverCommandDUnitTest {

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
  public void registerDriverSucceedsWithDriverClassName() {
    String URL = "jdbc:mysql://localhost/";

    IgnoredException.addIgnoredException(
        "No suitable driver");
    IgnoredException.addIgnoredException(
        "create data-source failed");

    final String jdbcJarName = "mysql-connector-java-8.0.15.jar";
    final String jdbcDriverClassName = "com.mysql.cj.jdbc.Driver.class";
    File mySqlDriverFile = loadTestResource("/" + jdbcJarName);
    assertThat(mySqlDriverFile).exists();
    String jarFile = mySqlDriverFile.getAbsolutePath();

    // attempt to create the data source without a deployed-jar to verify that the driver isn't on
    // the classpath by default
    gfsh.executeAndAssertThat(
        "create data-source --name=mySqlDataSource --username=mySqlUser --password=mySqlPass --pooled=false --url=\""
            + URL + "\"")
        .statusIsError();

    gfsh.executeAndAssertThat("deploy --jar=" + jarFile).statusIsSuccess();

    gfsh.executeAndAssertThat("register driver --driver-class=" + jdbcDriverClassName)
        .statusIsSuccess();

    IgnoredException.removeAllExpectedExceptions();
    IgnoredException.addIgnoredException(
        "create data-source failed");
    IgnoredException.addIgnoredException(
        "com.mysql.cj.jdbc.exceptions.CommunicationsException: Communications link failure");
    IgnoredException.addIgnoredException(
        "Access denied for user 'mySqlUser'@'localhost'");
    IgnoredException.addIgnoredException(
        "Failed to connect to \"mySqlDataSource\"");

    gfsh.executeAndAssertThat(
        "create data-source --name=mySqlDataSource --username=mySqlUser --password=mySqlPass --pooled=false --url=\""
            + URL + "\"")
        .statusIsError();



  }

  private File loadTestResource(String fileName) {
    String filePath = TestUtil.getResourcePath(this.getClass(), fileName);
    Assertions.assertThat(filePath).isNotNull();

    return new File(filePath);
  }
}
