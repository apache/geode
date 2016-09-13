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
package org.apache.geode.management.bean.stats;

import org.junit.experimental.categories.Category;
import org.junit.Test;

import static org.junit.Assert.*;

import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;

import java.util.Set;

import javax.management.ObjectName;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.internal.cache.DiskStoreStats;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.management.DistributedSystemMXBean;
import org.apache.geode.management.ManagementTestBase;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.management.internal.beans.MemberMBean;
import org.apache.geode.management.internal.beans.MemberMBeanBridge;
import org.apache.geode.test.dunit.Assert;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.VM;

/**
 */
@Category(DistributedTest.class)
public class DistributedSystemStatsDUnitTest extends ManagementTestBase{
  
  private static final long serialVersionUID = 1L;

  public DistributedSystemStatsDUnitTest() {
    super();
  }

  @Test
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
        DiskStoreStats diskStoreStats = new DiskStoreStats(basicGetSystem(), "test");
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
            Assert.fail("FAILED WITH EXCEPION", e);
          } catch (Exception e) {
            Assert.fail("FAILED WITH EXCEPION", e);
          }
        }

      }
    });
  }
}
