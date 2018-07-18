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
import static java.util.concurrent.TimeUnit.MINUTES;
import static javax.management.MBeanServerInvocationHandler.newProxyInstance;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;
import java.util.Set;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.Query;
import javax.management.QueryExp;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.internal.process.ControllableProcess;
import org.apache.geode.internal.process.ProcessType;
import org.apache.geode.management.MemberMXBean;

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
  public void queryWithNullFindsMemberMXBean() throws Exception {
    givenConstraint(null);

    whenQuerying(getPlatformMBeanServer());

    thenMemberMXBeanShouldBeFound().andShouldMatchCurrentMember();
  }

  @Test
  public void queryWithProcessIdFindsMemberMXBean() throws Exception {
    givenConstraint(Query.eq(Query.attr("ProcessId"), Query.value(localPid)));

    whenQuerying(getPlatformMBeanServer());

    thenMemberMXBeanShouldBeFound().andShouldMatchCurrentMember();
  }

  @Test
  public void queryWithMemberNameFindsMemberMXBean() throws Exception {
    givenConstraint(Query.eq(Query.attr("Name"), Query.value(getUniqueName())));

    whenQuerying(getPlatformMBeanServer());

    thenMemberMXBeanShouldBeFound().andShouldMatchCurrentMember();
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
    await().atMost(2, MINUTES)
        .until(() -> assertThat(mbeanServer.queryNames(pattern, null)).isNotEmpty());
  }
}
