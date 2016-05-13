/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.security;

import static org.assertj.core.api.Assertions.*;

import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.management.MemberMXBean;
import com.gemstone.gemfire.security.GemFireSecurityException;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(IntegrationTest.class)
public class DataCommandsSecurityTest {
  private static int jmxManagerPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);

  private MemberMXBean bean;

  @ClassRule
  public static JsonAuthorizationCacheStartRule serverRule = new JsonAuthorizationCacheStartRule(
      jmxManagerPort, "cacheServer.json");

  @Rule
  public MBeanServerConnectionRule connectionRule = new MBeanServerConnectionRule(jmxManagerPort);

  @Before
  public void setUp() throws Exception {
    bean = connectionRule.getProxyMBean(MemberMXBean.class);
  }

  @Test
  @JMXConnectionConfiguration(user = "region1-user", password = "1234567")
  public void testDataUser() throws Exception {
    bean.processCommand("locate entry --key=k1 --region=region1");

    // can't operate on secureRegion
    assertThatThrownBy(() -> bean.processCommand("locate entry --key=k1 --region=secureRegion")).isInstanceOf(GemFireSecurityException.class);
  }

  @JMXConnectionConfiguration(user = "secure-user", password = "1234567")
  @Test
  public void testSecureDataUser(){
    // can do all these on both regions
    bean.processCommand("locate entry --key=k1 --region=region1");
    bean.processCommand("locate entry --key=k1 --region=secureRegion");
  }

  // dataUser has all the permissions granted, but not to region2 (only to region1)
  @JMXConnectionConfiguration(user = "region1-user", password = "1234567")
  @Test
  public void testRegionAcess(){
    assertThatThrownBy(() -> bean.processCommand("rebalance --include-region=region2")).isInstanceOf(GemFireSecurityException.class)
        .hasMessageContaining(TestCommand.dataManage.toString());

    assertThatThrownBy(() -> bean.processCommand("export data --region=region2 --file=foo.txt --member=value")).isInstanceOf(GemFireSecurityException.class);
    assertThatThrownBy(() -> bean.processCommand("import data --region=region2 --file=foo.txt --member=value")).isInstanceOf(GemFireSecurityException.class);

    assertThatThrownBy(() -> bean.processCommand("put --key=key1 --value=value1 --region=region2")).isInstanceOf(GemFireSecurityException.class)
        .hasMessageContaining("DATA:WRITE:region2");

    assertThatThrownBy(() -> bean.processCommand("get --key=key1 --region=region2")).isInstanceOf(GemFireSecurityException.class)
        .hasMessageContaining("DATA:READ:region2");
    }

}
