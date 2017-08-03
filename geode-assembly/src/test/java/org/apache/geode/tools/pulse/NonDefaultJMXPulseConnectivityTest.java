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

package org.apache.geode.tools.pulse;

import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_BIND_ADDRESS;

import java.net.InetAddress;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class NonDefaultJMXPulseConnectivityTest extends AbstractPulseConnectivityTest {

  @BeforeClass
  public static void beforeClass() throws Exception {
    // Make sure we use something different than "localhost" for this test.
    jmxBindAddress = InetAddress.getLocalHost().getHostName();
    if ("localhost".equals(jmxBindAddress)) {
      jmxBindAddress = InetAddress.getLocalHost().getHostAddress();
    }

    Properties locatorProperties = new Properties();
    locatorProperties.setProperty(JMX_MANAGER_BIND_ADDRESS, jmxBindAddress);
    locator.withProperties(locatorProperties).startLocator();
  }
}
