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
 *
 */

package org.apache.geode.management.internal.security;

import static org.junit.Assert.assertEquals;

import org.apache.geode.test.dunit.rules.LocalServerStarterRule;
import org.apache.geode.test.dunit.rules.ServerStarterBuilder;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class JavaRmiServerNameTest {

  private static final String JMX_HOST = "myHostname";

  @ClassRule
  public static LocalServerStarterRule server = new ServerStarterBuilder()
      .withProperty("jmx-manager-hostname-for-clients", JMX_HOST).withJMXManager().buildInThisVM();

  /**
   * this is for GEODE-1548
   */
  @Test
  public void testThatJavaRmiServerNameGetsSet() {
    assertEquals(JMX_HOST, System.getProperty("java.rmi.server.hostname"));
  }

  @After
  public void after() {
    System.setProperty("java.rmi.server.hostname", "");
  }

}
