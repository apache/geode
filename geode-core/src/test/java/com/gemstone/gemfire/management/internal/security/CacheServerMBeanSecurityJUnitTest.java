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

import static com.gemstone.gemfire.management.internal.ManagementConstants.OBJECTNAME__CLIENTSERVICE_MXBEAN;
import static javax.management.JMX.newMBeanProxy;
import static org.assertj.core.api.Assertions.*;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.CacheServerMXBean;
import com.gemstone.gemfire.management.internal.beans.CacheServerMBean;
import com.gemstone.gemfire.util.test.TestUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class CacheServerMBeanSecurityJUnitTest {

  private static Cache cache;
  private static DistributedSystem ds;
  private JMXConnector jmxConnector;
  private MBeanServerConnection mbeanServer;
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private CacheServerMXBean cacheServerMXBean;

  @ClassRule
  public static RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @BeforeClass
  public static void beforeClassSetUp() throws Exception {
    System.setProperty(ResourceConstants.RESORUCE_SEC_DESCRIPTOR, TestUtil.getResourcePath(CacheServerMBeanSecurityJUnitTest.class, "cacheServer.json"));

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
  }

  @Before
  public void setUp() throws Exception {
  }

  @After
  public void tearDown() throws Exception {
    this.jmxConnector.close();
    this.jmxConnector = null;
  }

  @AfterClass
  public static void afterClassTearDown() throws Exception {
    cache.close();
    cache = null;
  }

  @Test
  public void sanity() throws Exception {
    createConnection("superuser", "1234567");
    assertThat(cache.getCacheServers()).hasSize(1);

    cacheServerMXBean.removeIndex("foo");
    cacheServerMXBean.executeContinuousQuery("bar");
  }

  private void createConnection(String username, String password) throws Exception {
    Map<String, String[]> env = new HashMap<>();
    env.put(JMXConnector.CREDENTIALS, new String[] {username, password});
    JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://:" + jmxManagerPort + "/jmxrmi");

    this.jmxConnector = JMXConnectorFactory.connect(url, env);
    this.mbeanServer = this.jmxConnector.getMBeanServerConnection();

    cache.addCacheServer().start();

    ObjectName objectNamePattern = ObjectName.getInstance("GemFire:service=CacheServer,*");
    ObjectInstance bean = (ObjectInstance) this.mbeanServer.queryMBeans(objectNamePattern, null).toArray()[0];
    ObjectName oName = bean.getObjectName();
    cacheServerMXBean = JMX.newMBeanProxy(this.mbeanServer, oName, CacheServerMXBean.class);
  }
}
