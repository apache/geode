/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

import static org.apache.geode.test.awaitility.GeodeAwaitility.await;

import java.io.Serializable;

import javax.management.ObjectName;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import org.apache.geode.cache.Cache;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.rules.serializable.SerializableTestName;

@Category({GfshTest.class})
public class MemberMXBeanDistributedTest implements
    Serializable {

  private static MemberVM locator, server1, server2, server3, server4;

  @ClassRule
  public static ClusterStartupRule lsRule = new ClusterStartupRule();

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TestName testName = new SerializableTestName();

  @BeforeClass
  public static void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    server1 = lsRule.startServerVM(1, "", locator.getPort());
    server2 = lsRule.startServerVM(2, "", locator.getPort());
    server3 = lsRule.startServerVM(3, "", locator.getPort());
    server4 = lsRule.startServerVM(4, "", locator.getPort());

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testBucketCount() throws InterruptedException {
    String regionName = "testCreateRegion";

    gfsh.executeAndAssertThat("create region"
        + " --name=" + regionName
        + " --type=PARTITION_PERSISTENT"
        + " --total-num-buckets=1000").statusIsSuccess();

    gfsh.executeAndAssertThat("query --query=\"select * from /" + regionName + "\"")
        .statusIsSuccess();

    server1.invoke(() -> waitBucketsToInitialize(245));
    server2.invoke(() -> waitBucketsToInitialize(245));
    server3.invoke(() -> waitBucketsToInitialize(245));
    server4.invoke(() -> waitBucketsToInitialize(245));

    for (int i = 1; i < 4; i++) {
      final String tempname = "/" + regionName + i;
      gfsh.executeAndAssertThat("create region"
          + " --name=" + regionName + i
          + " --type=PARTITION_PERSISTENT"
          + " --total-num-buckets=1000"
          + " --colocated-with=" + regionName).statusIsSuccess();
    }

    server1.invoke(() -> waitBucketsToInitialize(990));
    server2.invoke(() -> waitBucketsToInitialize(990));
    server3.invoke(() -> waitBucketsToInitialize(990));
    server4.invoke(() -> waitBucketsToInitialize(990));

  }

  private void waitBucketsToInitialize(int size) {
    Cache cache = ClusterStartupRule.getCache();

    DistributedMember member = cache.getDistributedSystem().getDistributedMember();
    ManagementService mgmtService = ManagementService.getManagementService(cache);

    ObjectName memberMBeanName = mgmtService.getMemberMBeanName(member);
    MemberMXBean memberMXBean = mgmtService.getMBeanInstance(memberMBeanName, MemberMXBean.class);

    await().until(() -> memberMXBean.getTotalBucketCount() >= size);
  }
}
