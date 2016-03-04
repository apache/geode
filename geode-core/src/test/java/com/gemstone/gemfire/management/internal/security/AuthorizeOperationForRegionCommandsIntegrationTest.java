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

import static org.jgroups.util.Util.readFile;
import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.remote.JMXPrincipal;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests <code>JSONAuthorization.authorizeOperation(...)</code> for Region commands.
 */
@Category(IntegrationTest.class)
@SuppressWarnings("deprecation")
public class AuthorizeOperationForRegionCommandsIntegrationTest {
  
  private GemFireCacheImpl cache;
  private DistributedSystem ds;
  private int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  @Rule
  public TestName testName = new TestName();
  
  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Before
  public void setUp() {
    Properties properties = new Properties();
    properties.put("name", testName.getMethodName());
    properties.put(DistributionConfig.LOCATORS_NAME, "");
    properties.put(DistributionConfig.MCAST_PORT_NAME, "0");
    properties.put(DistributionConfig.JMX_MANAGER_NAME, "true");
    properties.put(DistributionConfig.JMX_MANAGER_START_NAME, "true");
    properties.put(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(this.jmxManagerPort));
    properties.put(DistributionConfig.HTTP_SERVICE_PORT_NAME, "0");
    properties.put(DistributionConfig.SECURITY_CLIENT_ACCESSOR_NAME, JSONAuthorization.class.getName() + ".create");
    properties.put(DistributionConfig.SECURITY_CLIENT_AUTHENTICATOR_NAME, JSONAuthorization.class.getName() + ".create");

    this.ds = DistributedSystem.connect(properties);
    this.cache = (GemFireCacheImpl) CacheFactory.create(ds);
  }

  @After
  public void tearDown() {
    if (cache != null) {
      cache.close();
      cache = null;
    }
    if (ds != null) {
      ds.disconnect();
      ds = null;
    }
  }
  
  @Test
  public void testAuthorizeOperationWithRegionOperations() throws Exception {
    JSONAuthorization authorization = new JSONAuthorization("auth3.json");
    authorization.init(new JMXPrincipal("tushark"), null, null);
    
    checkAccessControlMBean();
    
    CLIOperationContext cliContext = new CLIOperationContext("locate entry --key=k1 --region=region1");
    boolean result = authorization.authorizeOperation(null, cliContext);
    assertTrue("Operation not authorized", result);

    cliContext = new CLIOperationContext("locate entry --key=k1 --region=secureRegion");
    result = authorization.authorizeOperation(null, cliContext);
    assertTrue("Operation not authorized", result);

    authorization.init(new JMXPrincipal("avinash"), null, null);
    result = authorization.authorizeOperation(null, cliContext);
    assertTrue(result);

    cliContext = new CLIOperationContext("locate entry --key=k1 --region=region1");
    result = authorization.authorizeOperation(null, cliContext);
    assertTrue(result);
  }

  private void checkAccessControlMBean() throws Exception {
    ObjectName name = new ObjectName(ManagementInterceptor.OBJECT_NAME_ACCESSCONTROL);
    MBeanServer platformMBeanServer = ManagementFactory.getPlatformMBeanServer();
    Set<ObjectName> names = platformMBeanServer.queryNames(name, null);
    assertFalse(names.isEmpty());
    assertEquals(1, names.size());
    assertEquals(name,names.iterator().next());
  }
}
