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
package com.gemstone.gemfire.distributed;

import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Tests querying of MemberMXBean which is used by MBeanProcessController to
 * control GemFire ControllableProcesses.
 * 
 * @since 8.0
 */
@Category(IntegrationTest.class)
public class LauncherMemberMXBeanJUnitTest extends AbstractLauncherJUnitTestCase {

  @Before
  public final void setUpLauncherMemberMXBeanTest() throws Exception {
  }

  @After
  public final void tearDownLauncherMemberMXBeanTest() throws Exception {
    InternalDistributedSystem ids = InternalDistributedSystem.getConnectedInstance();
    if (ids != null) {
      ids.disconnect();
    }
  }

  @Test
  public void testQueryForMemberMXBean() throws Exception {
    final Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("name", getUniqueName());
    new CacheFactory(props).create();
    
    final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName pattern = ObjectName.getInstance("GemFire:type=Member,*");

    waitForMemberMXBean(mbeanServer, pattern);
    
    final Set<ObjectName> mbeanNames = mbeanServer.queryNames(pattern, null);
    assertFalse(mbeanNames.isEmpty());
    assertEquals("mbeanNames=" + mbeanNames, 1, mbeanNames.size());
    
    final ObjectName objectName = mbeanNames.iterator().next();
    final MemberMXBean mbean = MBeanServerInvocationHandler.newProxyInstance(mbeanServer, objectName,
      MemberMXBean.class, false);

    assertNotNull(mbean);
    assertEquals(ProcessUtils.identifyPid(), mbean.getProcessId());
    assertEquals(getUniqueName(), mbean.getName());
    assertEquals(getUniqueName(), mbean.getMember());
  }
  
  @Test
  public void testQueryForMemberMXBeanWithProcessId() throws Exception {
    final Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("name", getUniqueName());
    new CacheFactory(props).create();
    
    final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName pattern = ObjectName.getInstance("GemFire:type=Member,*");
    final QueryExp constraint = Query.eq(Query.attr("ProcessId"),Query.value(ProcessUtils.identifyPid()));
    
    waitForMemberMXBean(mbeanServer, pattern);
    
    final Set<ObjectName> mbeanNames = mbeanServer.queryNames(pattern, constraint);
    assertFalse(mbeanNames.isEmpty());
    assertEquals(1, mbeanNames.size());
    
    final ObjectName objectName = mbeanNames.iterator().next();
    final MemberMXBean mbean = MBeanServerInvocationHandler.newProxyInstance(mbeanServer, objectName, MemberMXBean.class, false);

    assertNotNull(mbean);
    assertEquals(ProcessUtils.identifyPid(), mbean.getProcessId());
    assertEquals(getUniqueName(), mbean.getName());
    assertEquals(getUniqueName(), mbean.getMember());
  }
  
  @Test
  public void testQueryForMemberMXBeanWithMemberName() throws Exception {
    final Properties props = new Properties();
    props.setProperty("mcast-port", "0");
    props.setProperty("locators", "");
    props.setProperty("name", getUniqueName());
    new CacheFactory(props).create();
    
    final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName pattern = ObjectName.getInstance("GemFire:type=Member,*");
    final QueryExp constraint = Query.eq(Query.attr("Name"), Query.value(getUniqueName()));
    
    waitForMemberMXBean(mbeanServer, pattern);
    
    final Set<ObjectName> mbeanNames = mbeanServer.queryNames(pattern, constraint);
    assertFalse(mbeanNames.isEmpty());
    assertEquals(1, mbeanNames.size());
    
    final ObjectName objectName = mbeanNames.iterator().next();
    final MemberMXBean mbean = MBeanServerInvocationHandler.newProxyInstance(mbeanServer, objectName, MemberMXBean.class, false);

    assertNotNull(mbean);
    assertEquals(getUniqueName(), mbean.getMember());
  }
  
  private void waitForMemberMXBean(final MBeanServer mbeanServer, final ObjectName pattern) throws Exception {
    assertEventuallyTrue("waiting for MemberMXBean to be registered", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        Set<ObjectName> mbeanNames = mbeanServer.queryNames(pattern, null);
        return !mbeanNames.isEmpty();
      }
    }, WAIT_FOR_MBEAN_TIMEOUT, INTERVAL);
  }
}
