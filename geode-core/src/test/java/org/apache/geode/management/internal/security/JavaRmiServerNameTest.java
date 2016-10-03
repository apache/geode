/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.geode.management.internal.security;

import static org.apache.geode.distributed.ConfigurationProperties.*;
import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class JavaRmiServerNameTest {

  private static final String JMX_HOST = "myHostname";

  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  //https://issues.apache.org/jira/browse/GEODE-1548
  @Test
  public void testThatJavaRmiServerNameGetsSet() {
    Properties properties = new Properties();
    properties.put(LOCATORS, "");
    properties.put(MCAST_PORT, "0");
    properties.put(JMX_MANAGER, "true");
    properties.put(JMX_MANAGER_START, "true");
    properties.put(JMX_MANAGER_PORT, String.valueOf(jmxManagerPort));
    properties.put("jmx-manager-hostname-for-clients", JMX_HOST);

    new CacheFactory(properties).create();
    assertEquals(JMX_HOST, System.getProperty("java.rmi.server.hostname"));
  }

}
