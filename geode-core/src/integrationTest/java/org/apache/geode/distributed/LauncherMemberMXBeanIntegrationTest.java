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
package org.apache.geode.distributed;

import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static javax.management.MBeanServerInvocationHandler.newProxyInstance;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.Properties;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.JMX;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.openmbean.CompositeDataSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.process.ControllableProcess;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.management.JVMMetrics;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.management.OSMetrics;
import org.apache.geode.test.awaitility.GeodeAwaitility;

/**
 * Integration tests for querying of {@link MemberMXBean} as used in MBeanProcessController to
 * manage a {@link ControllableProcess}.
 *
 * <p>
 * This test is an experiment in using given/when/then custom methods for better BDD readability.
 *
 * @since GemFire 8.0
 */
public class LauncherMemberMXBeanIntegrationTest extends LauncherIntegrationTestCase {

  private ObjectName pattern;
  private QueryExp constraint;
  private Set<ObjectName> mbeanNames;
  private ObjectName mbeanObjectName;

  @Before
  public void setUp() throws Exception {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty("name", getUniqueName());
    new CacheFactory(props).create();

    pattern = ObjectName.getInstance("GemFire:type=Member,*");
    waitForMemberMXBean(getPlatformMBeanServer(), pattern);
  }

  @After
  public void tearDown() throws Exception {
    disconnectFromDS();
  }

  @Test
  public void queryWithNullFindsMemberMXBean() {
    givenConstraint(null);

    whenQuerying(getPlatformMBeanServer());

    thenMemberMXBeanShouldBeFound().andShouldMatchCurrentMember();
  }

  @Test
  public void queryWithProcessIdFindsMemberMXBean() {
    givenConstraint(Query.eq(Query.attr("ProcessId"), Query.value(localPid)));

    whenQuerying(getPlatformMBeanServer());

    thenMemberMXBeanShouldBeFound().andShouldMatchCurrentMember();
  }

  @Test
  public void queryWithMemberNameFindsMemberMXBean() {
    givenConstraint(Query.eq(Query.attr("Name"), Query.value(getUniqueName())));

    whenQuerying(getPlatformMBeanServer());

    thenMemberMXBeanShouldBeFound().andShouldMatchCurrentMember();
  }

  @Test
  public void showOSMetrics_reconstructsOSMetricsFromCompositeDataType()
      throws MBeanException, InstanceNotFoundException, ReflectionException {
    givenConstraint(Query.eq(Query.attr("Name"), Query.value(getUniqueName())));

    whenQuerying(getPlatformMBeanServer());
    assertThat(mbeanNames).hasSize(1);

    MemberMXBean mbean = getMXBeanProxy();

    CompositeDataSupport cds =
        (CompositeDataSupport) getPlatformMBeanServer().invoke(mbeanObjectName, "showOSMetrics",
            null, null);
    OSMetrics osMetrics = mbean.showOSMetrics();
    assertThat(osMetrics).isNotNull();

    Long osMetricsCommittedMemory = osMetrics.getCommittedVirtualMemorySize();
    float virtualMemoryRatio = osMetricsCommittedMemory.floatValue()
        / ((Long) cds.get("committedVirtualMemorySize")).floatValue();

    // On windows in particular, the memory value returned from the live bean has often already
    // changed from the statically recorded value.
    assertThat(virtualMemoryRatio).isCloseTo(virtualMemoryRatio, within(0.01F));

    // Verify conversion from CompositeData to OSMetrics
    assertThat(osMetrics.getArch()).isEqualTo(cds.get("arch"));
    assertThat(osMetrics.getAvailableProcessors()).isEqualTo(cds.get("availableProcessors"));
    assertThat(osMetrics.getFreePhysicalMemorySize()).isEqualTo(cds.get("freePhysicalMemorySize"));
    assertThat(osMetrics.getFreeSwapSpaceSize()).isEqualTo(cds.get("freeSwapSpaceSize"));
    assertThat(osMetrics.getMaxFileDescriptorCount()).isEqualTo(cds.get("maxFileDescriptorCount"));
    assertThat(osMetrics.getName()).isEqualTo(cds.get("name"));
    assertThat(osMetrics.getOpenFileDescriptorCount())
        .isEqualTo(cds.get("openFileDescriptorCount"));
    assertThat(osMetrics.getProcessCpuTime()).isEqualTo(cds.get("processCpuTime"));
    assertThat(osMetrics.getSystemLoadAverage()).isEqualTo(cds.get("systemLoadAverage"));
    assertThat(osMetrics.getTotalPhysicalMemorySize())
        .isEqualTo(cds.get("totalPhysicalMemorySize"));
    assertThat(osMetrics.getTotalSwapSpaceSize()).isEqualTo(cds.get("totalSwapSpaceSize"));
    assertThat(osMetrics.getVersion()).isEqualTo(cds.get("version"));
  }

  @Test
  public void showJVMMetrics_returnsOJVMMetricsType()
      throws MBeanException, InstanceNotFoundException, ReflectionException {
    givenConstraint(Query.eq(Query.attr("Name"), Query.value(getUniqueName())));

    whenQuerying(getPlatformMBeanServer());
    assertThat(mbeanNames).hasSize(1);

    MemberMXBean mbean = getMXBeanProxy();

    CompositeDataSupport cds =
        (CompositeDataSupport) getPlatformMBeanServer().invoke(mbeanObjectName, "showJVMMetrics",
            null, null);
    JVMMetrics jvmMetrics = mbean.showJVMMetrics();
    assertThat(jvmMetrics).isNotNull();
    assertThat(jvmMetrics.getCommittedMemory()).isEqualTo(cds.get("committedMemory"));
    assertThat(jvmMetrics.getGcCount()).isEqualTo(cds.get("gcCount"));
    assertThat(jvmMetrics.getGcTimeMillis()).isEqualTo(cds.get("gcTimeMillis"));
    assertThat(jvmMetrics.getInitMemory()).isEqualTo(cds.get("initMemory"));
    assertThat(jvmMetrics.getMaxMemory()).isEqualTo(cds.get("maxMemory"));
    assertThat(jvmMetrics.getTotalThreads()).isEqualTo(cds.get("totalThreads"));
    assertThat(jvmMetrics.getUsedMemory()).isEqualTo(cds.get("usedMemory"));
  }

  private MemberMXBean getMXBeanProxy() {
    this.mbeanObjectName = mbeanNames.iterator().next();
    return JMX.newMXBeanProxy(getPlatformMBeanServer(), mbeanObjectName, MemberMXBean.class, false);
  }

  private void givenConstraint(final QueryExp constraint) {
    this.constraint = constraint;
  }

  private void whenQuerying(final MBeanServer mbeanServer) {
    mbeanNames = mbeanServer.queryNames(pattern, constraint);
  }

  private LauncherMemberMXBeanIntegrationTest thenMemberMXBeanShouldBeFound() {
    assertThat(mbeanNames).hasSize(1);

    return this;
  }

  private LauncherMemberMXBeanIntegrationTest andShouldMatchCurrentMember() {
    ObjectName objectName = mbeanNames.iterator().next();
    MemberMXBean mbean =
        newProxyInstance(getPlatformMBeanServer(), objectName, MemberMXBean.class, false);

    assertThat(mbean.getMember()).isEqualTo(getUniqueName());
    assertThat(mbean.getName()).isEqualTo(getUniqueName());
    assertThat(mbean.getProcessId()).isEqualTo(localPid);

    return this;
  }

  @Override
  protected ProcessType getProcessType() {
    throw new UnsupportedOperationException(
        "getProcessType is not used by " + getClass().getSimpleName());
  }

  private void waitForMemberMXBean(final MBeanServer mbeanServer, final ObjectName pattern) {
    GeodeAwaitility.await()
        .untilAsserted(() -> assertThat(mbeanServer.queryNames(pattern, null)).isNotEmpty());
  }
}
