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

package org.apache.geode.management.internal.security;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;

import javax.management.remote.JMXConnector;

import org.apache.commons.lang3.JavaVersion;
import org.apache.commons.lang3.SystemUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.junit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;

public class JmxCredentialTypeTest {

  @ClassRule
  public static LocatorStarterRule locator = new LocatorStarterRule().withSecurityManager(
      SimpleSecurityManager.class).withAutoStart();

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule();

  @Test
  public void testWithNonStringCredential() throws Exception {
    Map<String, Object> env = new HashMap<>();
    env.put(JMXConnector.CREDENTIALS, new Integer(0));
    if (SystemUtils.isJavaVersionAtLeast(JavaVersion.JAVA_9)) {
      assertThatThrownBy(() -> connectionRule.connect("localhost", locator.getJmxPort(), env))
          .hasMessageContaining("filter status: REJECTED");
    } else {
      assertThatThrownBy(() -> connectionRule.connect("localhost", locator.getJmxPort(), env))
          .isInstanceOf(ClassCastException.class);
    }
  }
}
