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
package org.apache.geode.test.dunit.rules.tests;

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.LocatorStarterRule;

@Category(UnitTest.class)
public class MemberStarterRuleTest {

  private LocatorStarterRule locator;

  @After
  public void after() {
    if (locator != null)
      locator.after();
  }

  @Test
  public void testSetJMXPortWithProperty() {
    int port = 2000;
    locator = new LocatorStarterRule().withProperty(JMX_MANAGER_PORT, port + "");
    locator.before();
    assertThat(locator.getJmxPort()).isEqualTo(port);
  }

  @Test
  public void testSetJMXPortWithPropertyThenAPI() {
    int port = 2000;
    locator = new LocatorStarterRule().withProperty(JMX_MANAGER_PORT, port + "");

    // user call withJMXManager again
    locator.withJMXManager();
    locator.before();

    assertThat(locator.getJmxPort()).isEqualTo(port);
  }

  @Test
  public void testSetJMXPortWithAPIThenProperty() {
    // this first one wins
    locator = new LocatorStarterRule().withJMXManager();
    int port = locator.getJmxPort();

    locator.withProperty(JMX_MANAGER_PORT, "9999");
    locator.before();

    assertThat(locator.getJmxPort()).isEqualTo(port);
  }

  @Test
  public void testWithPort() {
    int targetPort = 12345;
    locator = new LocatorStarterRule().withPort(targetPort);
    locator.before();

    assertThat(locator.getPort()).isEqualTo(targetPort);
  }

  @Test
  public void testUseRandomPortByDefault() {
    locator = new LocatorStarterRule();
    locator.before();

    assertThat(locator.getJmxPort()).isNotEqualTo(1099);
    assertThat(locator.getJmxPort()).isNotEqualTo(-1);

    assertThat(locator.getHttpPort()).isNotEqualTo(7070);
    assertThat(locator.getHttpPort()).isNotEqualTo(-1);
  }

  @Test
  public void workingDirNotCreatedByDefault() throws Exception {
    String userDir = System.getProperty("user.dir");
    locator = new LocatorStarterRule();
    locator.before();
    assertThat(System.getProperty("user.dir")).isSameAs(userDir);
    assertThat(locator.getWorkingDir()).isNull();
  }

  @Test
  public void logFileDoesNotCreatesWorkingDir() throws Exception {
    locator = new LocatorStarterRule().withLogFile();
    locator.before();

    assertThat(locator.getName()).isNotNull();
    assertThat(locator.getWorkingDir()).isNull();
  }

  @Test
  public void workDirCreatesWorkDir() throws Exception {
    locator = new LocatorStarterRule().withWorkingDir();
    locator.before();

    assertThat(locator.getWorkingDir()).isNotNull();
  }
}
