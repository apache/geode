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
package com.gemstone.gemfire.management.bean.stats;

import java.util.Set;

import javax.management.ObjectName;

import com.gemstone.gemfire.distributed.DistributedMember;
import com.gemstone.gemfire.internal.cache.DiskStoreStats;
import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.management.DistributedSystemMXBean;
import com.gemstone.gemfire.management.ManagementTestBase;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.management.internal.SystemManagementService;
import com.gemstone.gemfire.management.internal.beans.MemberMBean;
import com.gemstone.gemfire.management.internal.beans.MemberMBeanBridge;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * @author rishim
 */
public class DistributedSystemStatsDUnitTest extends ManagementTestBase{
  
  private static final long serialVersionUID = 1L;

  public DistributedSystemStatsDUnitTest(String name) {
    super(name);
  }

  public void setUp() throws Exception {
    super.setUp();
  }

  public void tearDown2() throws Exception {
    super.tearDown2();
  }
  
  public void testDistributedSystemStats() throws Exception {
    initManagement(true);

    for(VM vm : managedNodeList){
      setDiskStats(vm);
    }
    verifyDiskStats(managingNode);
  }
  
  @SuppressWarnings("serial")
  public void setDiskStats(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Set Member Stats") {
      public void run() {
        MemberMBean bean = (MemberMBean) managementService.getMemberMXBean();
        MemberMBeanBridge bridge = bean.getBridge();
        DiskStoreStats diskStoreStats = new DiskStoreStats(system, "test");
        bridge.addDiskStoreStats(diskStoreStats);
        diskStoreStats.startRead();
        diskStoreStats.startWrite();
        diskStoreStats.startBackup();
        diskStoreStats.startRecovery();
        diskStoreStats.incWrittenBytes(20, true);
        diskStoreStats.startFlush();
        diskStoreStats.setQueueSize(10);
      }
    });
  }

  @SuppressWarnings("serial")
  public void verifyDiskStats(VM vm1) throws Exception {
    vm1.invoke(new SerializableRunnable("Set Member Stats") {
      public void run() {
        GemFireCacheImpl cache = GemFireCacheImpl.getInstance();
        
        SystemManagementService service = (SystemManagementService) getManagementService();
        DistributedSystemMXBean bean = service.getDistributedSystemMXBean();
        assertNotNull(bean);
        Set<DistributedMember> otherMemberSet = cache.getDistributionManager()
            .getOtherNormalDistributionManagerIds();
         
        for (DistributedMember member : otherMemberSet) {
          ObjectName memberMBeanName;
          try {
            memberMBeanName = service.getMemberMBeanName(member);
            waitForProxy(memberMBeanName, MemberMXBean.class);
            MemberMXBean memberBean = service.getMBeanProxy(memberMBeanName, MemberMXBean.class);
            waitForRefresh(2, memberMBeanName);
          } catch (NullPointerException e) {
            fail("FAILED WITH EXCEPION", e);
          } catch (Exception e) {
            fail("FAILED WITH EXCEPION", e);
          }
        }

      }
    });
  }
}
