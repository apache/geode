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

import static java.lang.management.ManagementFactory.getRuntimeMXBean;
import static org.apache.geode.cache.RegionShortcut.PARTITION_REDUNDANT;
import static org.apache.geode.cache.RegionShortcut.REPLICATE;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_TIME_STATISTICS;
import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_START;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLE_RATE;
import static org.apache.geode.distributed.ConfigurationProperties.STATISTIC_SAMPLING_ENABLED;
import static org.apache.geode.internal.process.ProcessUtils.identifyPidAsUnchecked;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.apache.geode.test.dunit.VM.getVM;
import static org.assertj.core.api.Assertions.assertThat;

import java.lang.management.ManagementFactory;
import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionFactory;
import org.apache.geode.cache30.CacheTestCase;
import org.apache.geode.internal.NanoTimer;
import org.apache.geode.internal.statistics.HostStatSampler;
import org.apache.geode.internal.statistics.SampleCollector;
import org.apache.geode.management.internal.SystemManagementService;
import org.apache.geode.test.dunit.rules.DistributedRestoreSystemProperties;

/**
 * Distributed tests for {@link MemberMXBean} attributes.
 */
@SuppressWarnings("serial")
public class MemberMXBeanAttributesDistributedTest extends CacheTestCase {

  private static final long BYTES_PER_MEGABYTE = 1024 * 1024;

  @Rule
  public DistributedRestoreSystemProperties restoreSystemProperties =
      new DistributedRestoreSystemProperties();

  @Before
  public void setUp() throws Exception {
    createMember();

    getVM(0).invoke(() -> {
      createManager();
      startManager();
    });
  }

  @Test
  public void testReplicateRegionAttributes() {
    RegionFactory<Number, Number> regionFactory = getCache().createRegionFactory(REPLICATE);
    regionFactory.create("testRegion1");
    regionFactory.create("testRegion2");
    regionFactory.create("testRegion3");

    Region<Number, Number> region1 = getCache().getRegion("/testRegion1");
    regionFactory.createSubregion(region1, "testSubRegion1");

    Region<Number, Number> region2 = getCache().getRegion("/testRegion2");
    regionFactory.createSubregion(region2, "testSubRegion2");

    Region<Number, Number> region3 = getCache().getRegion("/testRegion3");
    regionFactory.createSubregion(region3, "testSubRegion3");

    for (int i = 1; i < 1 + 200; i++) {
      region1.put(i, i);
      region2.put(i, i);
      region3.put(i, i);
    }

    sampleStatistics();

    MemberMXBean memberMXBean = getSystemManagementService().getMemberMXBean();

    assertThat(memberMXBean.getTotalRegionCount()).isEqualTo(6);
    assertThat(memberMXBean.getTotalRegionEntryCount()).isEqualTo(600);
    assertThat(memberMXBean.getRootRegionNames()).hasSize(3);
    assertThat(memberMXBean.listRegions()).hasSize(6);
  }

  @Test
  public void testPartitionedRegionAttributes() {
    RegionFactory<Number, Number> regionFactory =
        getCache().createRegionFactory(PARTITION_REDUNDANT);

    regionFactory.create("testPRRegion1");
    regionFactory.create("testPRRegion2");
    regionFactory.create("testPRRegion3");

    Region<Number, Number> region1 = getCache().getRegion("/testPRRegion1");
    Region<Number, Number> region2 = getCache().getRegion("/testPRRegion2");
    Region<Number, Number> region3 = getCache().getRegion("/testPRRegion3");

    for (int i = 1; i < 1 + 200; i++) {
      region1.put(i, i);
      region2.put(i, i);
      region3.put(i, i);
    }

    sampleStatistics();

    MemberMXBean memberMXBean = getSystemManagementService().getMemberMXBean();

    assertThat(memberMXBean.getPartitionRegionCount()).isEqualTo(3);
    assertThat(memberMXBean.getTotalBucketCount()).isEqualTo(339);
    assertThat(memberMXBean.getTotalPrimaryBucketCount()).isEqualTo(339);
  }

  @Test
  public void testOSAttributes() {
    MemberMXBean memberMXBean = getSystemManagementService().getMemberMXBean();

    await().untilAsserted(() -> assertThat(memberMXBean.getMemberUpTime()).isGreaterThan(0));

    assertThat(memberMXBean.getProcessId()).isEqualTo(identifyPidAsUnchecked());
    assertThat(memberMXBean.getClassPath()).isEqualTo(getRuntimeMXBean().getClassPath());
    assertThat(memberMXBean.getCurrentTime()).isGreaterThan(0);

    assertThat(memberMXBean.getUsedMemory()).isGreaterThan(10);
    assertThat(memberMXBean.getCurrentHeapSize()).isGreaterThan(10);

    assertThat(memberMXBean.getFreeMemory()).isGreaterThan(0);
    assertThat(memberMXBean.getFreeHeapSize()).isGreaterThan(0);

    assertThat(memberMXBean.getMaxMemory()).isEqualTo(getHeapMemoryUsageMegabytes());
    assertThat(memberMXBean.getMaximumHeapSize()).isEqualTo(getHeapMemoryUsageMegabytes());

    assertThat(memberMXBean.fetchJvmThreads().length).isGreaterThan(0);
  }

  @Test
  public void testConfigAttributes() {
    MemberMXBean memberMXBean = getSystemManagementService().getMemberMXBean();

    assertThat(memberMXBean.hasGatewayReceiver()).isFalse();
    assertThat(memberMXBean.hasGatewaySender()).isFalse();
    assertThat(memberMXBean.isLocator()).isFalse();
    assertThat(memberMXBean.isManager()).isFalse();
    assertThat(memberMXBean.isServer()).isFalse();
    assertThat(memberMXBean.isManagerCreated()).isFalse();
  }

  @Override
  public Properties getDistributedSystemProperties() {
    Properties props = new Properties();

    props.setProperty(ENABLE_TIME_STATISTICS, "true");
    props.setProperty(STATISTIC_SAMPLING_ENABLED, "true");
    props.setProperty(STATISTIC_SAMPLE_RATE, "60000");

    return props;
  }

  private void createMember() {
    getCache(getDistributedSystemProperties());
  }

  private void createManager() {
    Properties props = getDistributedSystemProperties();

    props.setProperty(JMX_MANAGER, "true");
    props.setProperty(JMX_MANAGER_START, "false");
    props.setProperty(JMX_MANAGER_PORT, "0");
    props.setProperty(HTTP_SERVICE_PORT, "0");

    getCache(props);
  }

  private void startManager() {
    SystemManagementService service = getSystemManagementService();
    service.createManager();
    service.startManager();
  }

  private SystemManagementService getSystemManagementService() {
    return (SystemManagementService) ManagementService.getManagementService(getCache());
  }

  private long getHeapMemoryUsageMegabytes() {
    return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax() / BYTES_PER_MEGABYTE;
  }

  private void sampleStatistics() {
    HostStatSampler sampler = getSystem().getStatSampler();
    SampleCollector sampleCollector = sampler.getSampleCollector();
    sampleCollector.sample(NanoTimer.getTime());
  }
}
