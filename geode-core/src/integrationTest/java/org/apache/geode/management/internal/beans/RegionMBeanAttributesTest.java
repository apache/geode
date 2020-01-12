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

package org.apache.geode.management.internal.beans;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.management.RegionMXBean;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

public class RegionMBeanAttributesTest {

  private RegionMXBean bean;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule // do not use a ClassRule since some test will do a shutdownMember
  public ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withRegion(RegionShortcut.REPLICATE, "FOO").withAutoStart();

  @Rule
  public MBeanServerConnectionRule mBeanRule = new MBeanServerConnectionRule();

  @Before
  public void setUp() throws Exception {
    gfsh.connectAndVerify(server.getJmxPort(), GfshCommandRule.PortType.jmxManager);
    mBeanRule.connect(server.getJmxPort());
  }

  @Test
  public void regionMBeanContainsAsyncEventQueueId() throws Exception {
    gfsh.executeAndAssertThat(
        "create async-event-queue --id=AEQ1 --listener=" + TestEventListener.class.getName())
        .statusIsSuccess();
    gfsh.executeAndAssertThat("alter region --name=FOO --async-event-queue-id=AEQ1")
        .statusIsSuccess();

    bean = mBeanRule.getProxyMXBean(RegionMXBean.class);

    assertThat(bean).isNotNull();
    Set<String> eventQueueIds = bean.listRegionAttributes().getAsyncEventQueueIds();
    assertThat(eventQueueIds).containsExactly("AEQ1");
  }

  @Test
  public void removingEventQueueAlsoRemovesFromMBean() throws Exception {
    gfsh.executeAndAssertThat(
        "create async-event-queue --id=AEQ1 --listener=" + TestEventListener.class.getName())
        .statusIsSuccess();
    gfsh.executeAndAssertThat("alter region --name=FOO --async-event-queue-id=AEQ1")
        .statusIsSuccess();

    bean = mBeanRule.getProxyMXBean(RegionMXBean.class);

    assertThat(bean).isNotNull();
    Set<String> eventQueueIds = bean.listRegionAttributes().getAsyncEventQueueIds();
    assertThat(eventQueueIds).containsExactly("AEQ1");

    gfsh.executeAndAssertThat("alter region --name=/FOO --async-event-queue-id=").statusIsSuccess();

    eventQueueIds = bean.listRegionAttributes().getAsyncEventQueueIds();
    assertThat(eventQueueIds).isNotNull().isEmpty();
  }
}
