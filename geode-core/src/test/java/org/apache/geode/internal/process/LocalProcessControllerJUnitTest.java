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
package org.apache.geode.internal.process;

import static org.junit.Assert.*;

import java.lang.management.ManagementFactory;
import java.util.Set;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanOperationInfo;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.internal.process.mbean.Process;
import org.apache.geode.test.junit.categories.UnitTest;

/**
 * Unit tests for LocalProcessController.
 * 
 * @since GemFire 7.0
 */
@Category(UnitTest.class)
public class LocalProcessControllerJUnitTest {

  private MBeanServer server;
  private ObjectName objectName;
  private int pid;

  @Rule
  public TestName testName = new TestName();

  @Before
  public void setUp() throws Exception {
    pid = ProcessUtils.identifyPid();
    final Process process = new Process(pid, true);

    this.objectName = ObjectName.getInstance(getClass().getSimpleName() + ":testName=" + testName.getMethodName());
    this.server = ManagementFactory.getPlatformMBeanServer();

    final ObjectInstance instance = this.server.registerMBean(process, objectName);
    assertNotNull(instance);
  }

  @After
  public void tearDown() throws Exception {
    this.server.unregisterMBean(objectName);
  }
  
  @Test
  public void testProcessMBean() throws Exception {
    // validate basics of the ProcessMBean
    Set<ObjectName> mbeanNames = this.server.queryNames(objectName, null);
    assertFalse("Zero matching mbeans", mbeanNames.isEmpty());
    assertEquals(1, mbeanNames.size());
    final ObjectName name = mbeanNames.iterator().next();
    
    final MBeanInfo info = this.server.getMBeanInfo(name);
    
    final MBeanOperationInfo[] operInfo = info.getOperations();
    assertEquals(1, operInfo.length);
    assertEquals("stop", operInfo[0].getName());
    
    final MBeanAttributeInfo[] attrInfo = info.getAttributes();
    assertEquals(2, attrInfo.length);
    // The order of these attributes is indeterminate
    assertTrue("Pid".equals(attrInfo[0].getName()) || "Process".equals(attrInfo[0].getName()));
    assertTrue("Pid".equals(attrInfo[1].getName()) || "Process".equals(attrInfo[1].getName()));
    assertNotNull(this.server.getAttribute(name, "Pid"));
    assertNotNull(this.server.getAttribute(name, "Process"));
    
    assertEquals(pid, this.server.getAttribute(name, "Pid"));
    assertEquals(true, this.server.getAttribute(name, "Process"));

    // validate query using only Pid attribute
    QueryExp constraint = Query.eq(
        Query.attr("Pid"),
        Query.value(pid));
    mbeanNames = this.server.queryNames(objectName, constraint);
    assertFalse("Zero matching mbeans", mbeanNames.isEmpty());
    
    // validate query with wrong Pid finds nothing
    constraint = Query.eq(
        Query.attr("Pid"),
        Query.value(pid+1));
    mbeanNames = this.server.queryNames(objectName, constraint);
    assertTrue("Found matching mbeans", mbeanNames.isEmpty());
    
    // validate query using both attributes
    constraint = Query.and(
        Query.eq(Query.attr("Process"),Query.value(true)),
        Query.eq(Query.attr("Pid"),Query.value(pid)));
    mbeanNames = this.server.queryNames(objectName, constraint);
    assertFalse("Zero matching mbeans", mbeanNames.isEmpty());
    
    // validate query with wrong attribute finds nothing
    constraint = Query.and(
        Query.eq(Query.attr("Process"),Query.value(false)),
        Query.eq(Query.attr("Pid"),Query.value(pid)));
    mbeanNames = this.server.queryNames(objectName, constraint);
    assertTrue("Found matching mbeans", mbeanNames.isEmpty());
  }
}
