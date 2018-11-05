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
package org.apache.geode.session.tests;

import org.junit.BeforeClass;

import org.apache.geode.test.dunit.DUnitEnv;

/**
 * Tomcat 9 Client Server tests
 *
 * Runs all the tests in {@link CargoTestBase} on the Tomcat 9 install, setup in the
 * {@link #setupTomcatInstall()} method before tests are run.
 */
public class Tomcat9ClientServerTest extends TomcatClientServerTest {
  private static ContainerInstall install;

  @BeforeClass
  public static void setupTomcatInstall() throws Exception {
    install = new TomcatInstall(TomcatInstall.TomcatVersion.TOMCAT9,
        ContainerInstall.ConnectionType.CLIENT_SERVER,
        ContainerInstall.DEFAULT_INSTALL_DIR + "Tomcat9ClientServerTest");
    install.setDefaultLocator(DUnitEnv.get().getLocatorAddress(), DUnitEnv.get().getLocatorPort());
  }

  @Override
  public ContainerInstall getInstall() {
    return install;
  }
}
