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
package org.apache.geode.management.internal.cli.commands.lifecycle;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.test.junit.categories.UnitTest;

public class StartJConsoleCommandTest {
  @Test
  public void testCreateJmxServerUrlWithMemberName() {
    assertEquals("service:jmx:rmi://localhost:8192/jndi/rmi://localhost:8192/jmxrmi",
        new StartJConsoleCommand().getJmxServiceUrlAsString("localhost[8192]"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCreateJmxServiceUrlWithInvalidMemberName() {
    try {
      System.err.println(new StartJConsoleCommand().getJmxServiceUrlAsString("memberOne[]"));
    } catch (IllegalArgumentException expected) {
      assertEquals(CliStrings.START_JCONSOLE__CONNECT_BY_MEMBER_NAME_ID_ERROR_MESSAGE,
          expected.getMessage());
      throw expected;
    }
  }
}
