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

package org.apache.geode.management.internal.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.management.MemberMXBean;
import org.apache.geode.security.NotAuthorizedException;
import org.apache.geode.test.junit.categories.SecurityTest;
import org.apache.geode.test.junit.rules.MBeanServerConnectionRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;

@Category({SecurityTest.class})
public class JmxMultipleConnectionsTest {
  @ClassRule
  public static ServerStarterRule server = new ServerStarterRule().withJMXManager()
      .withSecurityManager(SimpleSecurityManager.class)
      .withRegion(RegionShortcut.REPLICATE, "region1")
      .withAutoStart();

  @Test
  public void testMultipleJMXConnection() throws Exception {
    MBeanServerConnectionRule con1 = new MBeanServerConnectionRule();
    con1.connect(server.getJmxPort(), "dataReadRegion1", "dataReadRegion1");
    MemberMXBean bean1 = con1.getProxyMXBean(MemberMXBean.class);


    MBeanServerConnectionRule con2 = new MBeanServerConnectionRule();
    con2.connect(server.getJmxPort(), "dataReadRegion2", "dataReadRegion2");
    MemberMXBean bean2 = con2.getProxyMBean(MemberMXBean.class);

    System.out.println("using con2");
    assertThatThrownBy(() -> bean2.processCommand("locate entry --key=k1 --region=region1"))
        .isInstanceOf(NotAuthorizedException.class);

    System.out.println("using con1");
    assertThat(bean1.processCommand("locate entry --key=k1 --region=region1")).isNotNull();

    // this is supposed to log out user "dataReadRegion2", but it logs out "dataReadRegion1" because
    // that's the user on the thread
    System.out.println("logging out con2");
    con2.disconnect();

    System.out.println("using con1 again");
    assertThat(bean1.processCommand("locate entry --key=k1 --region=region1")).isNotNull();

    System.out.println("logging out con1");
    con1.disconnect();
  }
}
