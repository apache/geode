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
package org.apache.geode.management;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.lang.management.ManagementFactory;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.process.PidUnavailableException;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.test.dunit.LogWriterUtils;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

/**
 * This test class checks around 89 attributes of Member MBeans
 *
 */
@Category(DistributedTest.class)
public class MemberMBeanAttributesDUnitTest extends ManagementTestBase {
  

  private static final long serialVersionUID = 1L;
  
  /**
   * Factor converting bytes to MB
   */
  private static final long MBFactor = 1024 * 1024;

  // This must be bigger than the dunit ack-wait-threshold for the revoke
  // tests. The command line is setting the ack-wait-threshold to be
  // 60 seconds.
  private static final int MAX_WAIT = 70 * 1000;
  
  protected static final long SLEEP = 100;



  public MemberMBeanAttributesDUnitTest() {
    super();
  }

  protected void sample(VM vm1) {
    vm1.invoke(new SerializableRunnable("Create Cache") {
      public void run() {
        InternalDistributedSystem.getConnectedInstance().getStatSampler()
            .getSampleCollector().sample(NanoTimer.getTime());
      }

    });
  }
 

  
  @Test
  public void testReplRegionAttributes() throws Exception{
    initManagement(false);    
    setupForReplicateRegonAttributes(managedNodeList.get(0), 1);
    setupForReplicateRegonAttributes(managedNodeList.get(1), 201);    
    sample(managedNodeList.get(1));// Sample now    
    isReplicatedRegionAttrsOK(managedNodeList.get(1));  
    
  }
  
  
  @Test
  public void testPRRegionAttributes() throws Exception{
    initManagement(false);    
    setupForPartitionedRegonAttributes(managedNodeList.get(0), 1);    
    sample(managedNodeList.get(0));// Sample now    
    isPartitionedRegionAttrsOK(managedNodeList.get(0));  
    
  }
  
  @Test
  public void testOSAttributes() throws Exception{
    initManagement(false);
    isOSRelatedAttrsOK(managedNodeList.get(0));
  }
  
  @Test
  public void testConfigAttributes() throws Exception {
    initManagement(false);
    isConfigRelatedAttrsOK(managedNodeList.get(0));
  }

  
  public void setupForReplicateRegonAttributes(VM vm1 , final int offset) throws Exception {
    vm1.invoke(new SerializableRunnable("Create Cache") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        RegionFactory rf = cache.createRegionFactory(RegionShortcut.REPLICATE);
        LogWriterUtils.getLogWriter().info("Creating Dist Region");
        rf.create("testRegion1");
        rf.create("testRegion2");
        rf.create("testRegion3");

        
        Region r1 = cache.getRegion("/testRegion1");
        rf.createSubregion(r1, "testSubRegion1");
        
        Region r2 = cache.getRegion("/testRegion2");
        rf.createSubregion(r2, "testSubRegion2");
        
        Region r3 = cache.getRegion("/testRegion3");
        rf.createSubregion(r3, "testSubRegion3");
        
        
        for(int i = offset ; i< offset + 200 ; i++){
          r1.put(new Integer(i), new Integer(i));
          r2.put(new Integer(i), new Integer(i));
          r3.put(new Integer(i), new Integer(i));
        }        
        
      }
    });

  }
  
  public void setupForPartitionedRegonAttributes(VM vm1 , final int offset) throws Exception {
    vm1.invoke(new SerializableRunnable("Create Cache") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();               
        RegionFactory prRF = cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT);
        
        prRF.create("testPRRegion1");
        prRF.create("testPRRegion2");
        prRF.create("testPRRegion3");
        
        Region pr1 = cache.getRegion("/testPRRegion1");
        Region pr2 = cache.getRegion("/testPRRegion2");
        Region pr3 = cache.getRegion("/testPRRegion3");
        
        for(int i = offset ; i< offset + 200 ; i++){
          pr1.put(new Integer(i), new Integer(i));
          pr2.put(new Integer(i), new Integer(i));
          pr3.put(new Integer(i), new Integer(i));
        }
        
        
      }
    });

  }
  
  /**
   * This will check all the attributes which does not depend on any distribution message.
   * @param vm1
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void isPartitionedRegionAttrsOK(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Create Cache") {
      public void run() {
        MemberMXBean bean = managementService.getMemberMXBean();
        assertEquals(3 , bean.getPartitionRegionCount());
        assertEquals(339, bean.getTotalBucketCount());
        assertEquals(339, bean.getTotalPrimaryBucketCount());

      }
    });

  }
  
  /**
   * This will check all the attributes which does not depend on any distribution message.
   * @param vm1
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void isReplicatedRegionAttrsOK(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Create Cache") {
      public void run() {
        MemberMXBean bean = managementService.getMemberMXBean();

        assertEquals(6,bean.getTotalRegionCount()); 
        assertEquals(1200,bean.getTotalRegionEntryCount());

        assertEquals(3 ,bean.getRootRegionNames().length);
        assertEquals(600, bean.getInitialImageKeysReceived());  
        assertEquals(6 ,bean.listRegions().length);
      }
    });

  }
  
  /**
   * This will check all the attributes which does not depend on any distribution message.
   * @param vm1
   * @throws Exception
   */
  @SuppressWarnings("serial")
  public void isOSRelatedAttrsOK(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Create Cache") {
      public void run() {
        MemberMXBean bean = managementService.getMemberMXBean();

        try {
          assertEquals(ProcessUtils.identifyPid(), bean
              .getProcessId());
        } catch (PidUnavailableException e) {
          e.printStackTrace();
        }
        assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), bean
            .getClassPath());

        assertTrue(bean.getCurrentTime() > 0);
        // Sleep for one second
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        assertTrue(bean.getMemberUpTime() > 0);
        assertTrue(bean.getCurrentHeapSize() > 10);
        assertTrue(bean.getFreeHeapSize() > 0);
        assertEquals(bean.getMaximumHeapSize(), ManagementFactory
            .getMemoryMXBean().getHeapMemoryUsage().getMax()/MBFactor);
        
        // @TODO  Need more definitive test case
        assertTrue(bean.fetchJvmThreads().length > 0);
        
        // @TODO  Need more definitive test case
        //System.out.println(" CPU Usage is "+ bean.getCpuUsage());
        //assertTrue(bean.getCpuUsage() > 0.0f);
        
        //bean.getFileDescriptorLimit() 
      }
    });

  }
  
  @SuppressWarnings("serial")
  public void isConfigRelatedAttrsOK(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Create Cache") {
      public void run() {
        MemberMXBean bean = managementService.getMemberMXBean();      
        
        assertFalse(bean.hasGatewayReceiver());
        assertFalse(bean.hasGatewaySender());
        assertFalse(bean.isLocator());
        assertFalse(bean.isManager());
        assertFalse(bean.isServer());
        assertFalse(bean.isManagerCreated());
    

      }
    });

  }
}
