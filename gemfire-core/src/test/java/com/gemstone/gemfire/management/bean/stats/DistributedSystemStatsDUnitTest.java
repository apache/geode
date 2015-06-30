/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
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

import dunit.SerializableRunnable;
import dunit.VM;

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
