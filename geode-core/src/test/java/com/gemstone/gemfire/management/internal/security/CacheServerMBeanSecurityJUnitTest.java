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
 */
package com.gemstone.gemfire.management.internal.security;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.util.test.TestUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CacheServerMBeanSecurityJUnitTest {

  private static Cache cache;
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private CacheServerMXBean cacheServerMXBean;

  @Rule
  public MXBeanCreationRule<CacheServerMXBean> mxRule = new MXBeanCreationRule(jmxManagerPort, CacheServerMXBean.class, "GemFire:service=CacheServer,*");

  @ClassRule
  public static RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @BeforeClass
  public static void beforeClassSetUp() throws Exception {
    System.setProperty(ResourceConstants.RESOURCE_SEC_DESCRIPTOR,
        TestUtil.getResourcePath(CacheServerMBeanSecurityJUnitTest.class, "cacheServer.json"));

    Properties properties = new Properties();
    properties.put(DistributionConfig.NAME_NAME, CacheServerMBeanSecurityJUnitTest.class.getSimpleName());
    properties.put(DistributionConfig.LOCATORS_NAME, "");
    properties.put(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.put(DistributionConfig.JMX_MANAGER_NAME, "true");
    properties.put(DistributionConfig.JMX_MANAGER_START_NAME, "true");
    properties.put(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(jmxManagerPort));
    properties.put(DistributionConfig.HTTP_SERVICE_PORT_NAME, "0");
    properties.put(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME, JSONAuthorization.class.getName() + ".create");
    properties.put(DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME,
        JSONAuthorization.class.getName() + ".create");

    cache = new CacheFactory(properties).create();
    cache.addCacheServer().start();
  }

  @Before
  public void setUp() throws Exception {
    assertThat(cache.getCacheServers()).hasSize(1);
    cacheServerMXBean = mxRule.getProxyMBean();
  }

  @AfterClass
  public static void afterClassTearDown() throws Exception {
    cache.close();
    cache = null;
  }

  @Test
  @JMXConnectionConfiguration(user = "superuser", password = "1234567")
  public void testAllAccess() throws Exception {
    cacheServerMXBean.removeIndex("foo");
    cacheServerMXBean.executeContinuousQuery("bar");
  }

  @Test
  @JMXConnectionConfiguration(user = "stranger", password = "1234567")
  public void testNoAccess() throws Exception {
    assertThatThrownBy(() -> cacheServerMXBean.removeIndex("foo")).isInstanceOf(SecurityException.class);
    assertThatThrownBy(() -> cacheServerMXBean.executeContinuousQuery("bar")).isInstanceOf(SecurityException.class);
  }
}
