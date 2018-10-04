/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management;

import static org.apache.geode.management.MXBeanAwaitility.getSystemManagementService;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.lang.management.ManagementFactory;

import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.process.ProcessUtils;
import org.apache.geode.internal.statistics.HostStatSampler;
import org.apache.geode.internal.statistics.SampleCollector;
import org.apache.geode.test.awaitility.GeodeAwaitility;
import org.apache.geode.test.dunit.VM;

/**
 * Distributed tests for {@link MemberMXBean} attributes.
 */

@SuppressWarnings({"serial", "unused"})
public class MemberMBeanAttributesDUnitTest extends ManagementTestBase {

  private static final long BYTES_PER_MEGABYTE = 1024 * 1024;

  @Test
  public void testReplRegionAttributes() throws Exception {
    initManagement(false);
    setupForReplicateRegionAttributes(managedNodeList.get(0), 1);
    setupForReplicateRegionAttributes(managedNodeList.get(1), 201);

    sampleStatistics(managedNodeList.get(1));// Sample now

    validateReplicateRegionAttributes(managedNodeList.get(1));
  }

  @Test
  public void testPRRegionAttributes() throws Exception {
    initManagement(false);
    setupForPartitionedRegionAttributes(managedNodeList.get(0), 1);

    sampleStatistics(managedNodeList.get(0));// Sample now

    validatePartitionedRegionAttributes(managedNodeList.get(0));
  }

  @Test
  public void testOSAttributes() throws Exception {
    initManagement(false);

    validateSystemAndOSAttributes(managedNodeList.get(0));
  }

  @Test
  public void testConfigAttributes() throws Exception {
    initManagement(false);

    validateConfigAttributes(managedNodeList.get(0));
  }

  private void sampleStatistics(final VM vm) {
    vm.invoke("sampleStatistics", () -> {
      InternalDistributedSystem system = getInternalDistributedSystem();
      HostStatSampler sampler = system.getStatSampler();
      SampleCollector sampleCollector = sampler.getSampleCollector();
      sampleCollector.sample(NanoTimer.getTime());
    });
  }

  private void setupForReplicateRegionAttributes(final VM vm, final int offset) {
    vm.invoke("setupForReplicateRegionAttributes", () -> {
      Cache cache = getInternalCache();

      RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.REPLICATE);
      regionFactory.create("testRegion1");
      regionFactory.create("testRegion2");
      regionFactory.create("testRegion3");

      Region region1 = cache.getRegion("/testRegion1");
      regionFactory.createSubregion(region1, "testSubRegion1");

      Region region2 = cache.getRegion("/testRegion2");
      regionFactory.createSubregion(region2, "testSubRegion2");

      Region region3 = cache.getRegion("/testRegion3");
      regionFactory.createSubregion(region3, "testSubRegion3");

      for (int i = offset; i < offset + 200; i++) {
        region1.put(i, i);
        region2.put(i, i);
        region3.put(i, i);
      }
    });
  }

  private void setupForPartitionedRegionAttributes(final VM vm, final int offset) {
    vm.invoke("setupForPartitionedRegionAttributes", () -> {
      Cache cache = getInternalCache();
      RegionFactory regionFactory = cache.createRegionFactory(RegionShortcut.PARTITION_REDUNDANT);

      regionFactory.create("testPRRegion1");
      regionFactory.create("testPRRegion2");
      regionFactory.create("testPRRegion3");

      Region region1 = cache.getRegion("/testPRRegion1");
      Region region2 = cache.getRegion("/testPRRegion2");
      Region region3 = cache.getRegion("/testPRRegion3");

      for (int i = offset; i < offset + 200; i++) {
        region1.put(i, i);
        region2.put(i, i);
        region3.put(i, i);
      }
    });
  }

  /**
   * This will check all the attributes which does not depend on any distribution message.
   */
  private void validatePartitionedRegionAttributes(final VM vm) {
    vm.invoke("validatePartitionedRegionAttributes", () -> {
      MemberMXBean memberMXBean = getSystemManagementService().getMemberMXBean();

      assertEquals(3, memberMXBean.getPartitionRegionCount());
      assertEquals(339, memberMXBean.getTotalBucketCount());
      assertEquals(339, memberMXBean.getTotalPrimaryBucketCount());
    });
  }

  /**
   * This will check all the attributes which does not depend on any distribution message.
   */
  private void validateReplicateRegionAttributes(final VM vm) {
    vm.invoke("validateReplicateRegionAttributes", () -> {
      MemberMXBean memberMXBean = getSystemManagementService().getMemberMXBean();

      assertEquals(6, memberMXBean.getTotalRegionCount());
      assertEquals(1200, memberMXBean.getTotalRegionEntryCount());

      assertEquals(3, memberMXBean.getRootRegionNames().length);
      assertEquals(600, memberMXBean.getInitialImageKeysReceived());
      assertEquals(6, memberMXBean.listRegions().length);
    });
  }

  /**
   * This will check all the attributes which does not depend on any distribution message.
   */
  private void validateSystemAndOSAttributes(final VM vm) {
    vm.invoke("validateSystemAndOSAttributes", () -> {
      MemberMXBean memberMXBean = getSystemManagementService().getMemberMXBean();

      assertThat(memberMXBean.getProcessId()).isEqualTo(ProcessUtils.identifyPid());
      assertThat(memberMXBean.getClassPath()).isEqualTo(getClassPath());
      assertThat(memberMXBean.getCurrentTime()).isGreaterThan(0);

      GeodeAwaitility.await()
          .untilAsserted(() -> assertThat(memberMXBean.getMemberUpTime()).isGreaterThan(0));

      assertThat(memberMXBean.getUsedMemory()).isGreaterThan(10);
      assertThat(memberMXBean.getCurrentHeapSize()).isGreaterThan(10);

      assertThat(memberMXBean.getFreeMemory()).isGreaterThan(0);
      assertThat(memberMXBean.getFreeHeapSize()).isGreaterThan(0);

      assertThat(memberMXBean.getMaxMemory()).isEqualTo(getHeapMemoryUsageMegabytes());
      assertThat(memberMXBean.getMaximumHeapSize()).isEqualTo(getHeapMemoryUsageMegabytes());

      assertThat(memberMXBean.fetchJvmThreads().length).isGreaterThan(0);

      // TODO: provide better/more validation
      // System.out.println(" CPU Usage is "+ bean.getCpuUsage());
      // assertTrue(bean.getCpuUsage() > 0.0f);

      // bean.getFileDescriptorLimit()
    });
  }

  private void validateConfigAttributes(final VM vm) {
    vm.invoke("validateConfigAttributes", () -> {
      MemberMXBean memberMXBean = getSystemManagementService().getMemberMXBean();

      assertFalse(memberMXBean.hasGatewayReceiver());
      assertFalse(memberMXBean.hasGatewaySender());
      assertFalse(memberMXBean.isLocator());
      assertFalse(memberMXBean.isManager());
      assertFalse(memberMXBean.isServer());
      assertFalse(memberMXBean.isManagerCreated());
    });
  }

  private String getClassPath() {
    return ManagementFactory.getRuntimeMXBean().getClassPath();
  }

  private long getHeapMemoryUsageMegabytes() {
    return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() / BYTES_PER_MEGABYTE;
  }

  private InternalDistributedSystem getInternalDistributedSystem() {
    return InternalDistributedSystem.getConnectedInstance();
  }

  private InternalCache getInternalCache() {
    return GemFireCacheImpl.getInstance();
  }
}
