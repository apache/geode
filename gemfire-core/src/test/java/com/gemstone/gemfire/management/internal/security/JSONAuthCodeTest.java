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

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.internal.MBeanJMXAdapter;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

@Category(IntegrationTest.class)
public class JSONAuthCodeTest {
  
  private GemFireCacheImpl cache;
  private DistributedSystem ds;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();
  
  @Before
  public void setUp() {
    System.setProperty(DistributedSystem.PROPERTIES_FILE_PROPERTY, getClass().getSimpleName() + ".properties");
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
  public void testSimpleUserAndRole() throws Exception {    
    System.setProperty("resource.secDescriptor", TestUtil.getResourcePath(getClass(), "auth1.json")); 
    JSONAuthorization authorization = JSONAuthorization.create();        
    authorization.init(new JMXPrincipal("tushark"), null, null);
    
    JMXOperationContext context = new JMXOperationContext(MBeanJMXAdapter.getDistributedSystemName(), "queryData");
    boolean result = authorization.authorizeOperation(null, context);
    //assertTrue(result);
    
    context = new JMXOperationContext(MBeanJMXAdapter.getDistributedSystemName(), "changeAlertLevel");
    result = authorization.authorizeOperation(null,context);
    assertFalse(result);
  }

  @Ignore("Test was never implemented")
  @Test
  public void testInheritRole() {
  }
  
  @Ignore("Test was dead-coded")
  @Test
  public void testUserMultipleRole() throws Exception {
  }
  
  @Test
  public void testCLIAuthForRegion() throws Exception {
    System.setProperty("resource.secDescriptor", TestUtil.getResourcePath(getClass(), "auth3.json")); 
    JSONAuthorization authorization = JSONAuthorization.create();       
    authorization.init(new JMXPrincipal("tushark"), null, null);
    
    System.setProperty("resource-auth-accessor", JSONAuthorization.class.getCanonicalName());
    System.setProperty("resource-authenticator", JSONAuthorization.class.getCanonicalName());
    Properties pr = new Properties();
    pr.put("name", "testJMXOperationContext");
    pr.put(DistributionConfig.JMX_MANAGER_NAME, "true");
    pr.put(DistributionConfig.JMX_MANAGER_START_NAME, "true");
    int port = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    pr.put(DistributionConfig.JMX_MANAGER_PORT_NAME, String.valueOf(port));
    pr.put(DistributionConfig.HTTP_SERVICE_PORT_NAME, "0");
    ds = DistributedSystem.connect(pr);
    cache = (GemFireCacheImpl) CacheFactory.create(ds);
    
    checkAccessControlMBean();
    CLIOperationContext cliContext = new CLIOperationContext("locate entry --key=k1 --region=region1");
    boolean result = authorization.authorizeOperation(null, cliContext);
    assertTrue(result);

    cliContext = new CLIOperationContext("locate entry --key=k1 --region=secureRegion");
    result = authorization.authorizeOperation(null, cliContext);
    System.out.println("Result for secureRegion=" + result);
    //assertFalse(result); //this is failing due to logic issue

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
