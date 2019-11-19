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
package org.apache.geode;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.apache.geode.examples.SimpleSecurityManager;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class StressNewTestDunitTest {

  @Rule
  public ClusterStartupRule cluster = new ClusterStartupRule();

  @Before
  public void setUp() {
    MemberVM locator = cluster.startLocatorVM(0, l -> l.withSecurityManager(SimpleSecurityManager.class));
    int locatorPort = locator.getPort();
    List<MemberVM> servers = new ArrayList<>();
    IntStream.range(0, 2).forEach(
        i -> servers.add(i, cluster.startServerVM(i + 1, s -> s.withConnectionToLocator(locatorPort)
            .withCredential("clusterManage", "clusterManage"))));
  }

  @Test
  public void test1() {

  }

  @Test
  public void test2() {

  }

  @Test
  public void test3() {

  }

  @Test
  public void test4() {

  }

  @Test
  public void test5() {

  }

  @Test
  public void test6() {

  }

  @Test
  public void test7() {
//    int locatorPort = cluster.getMember(0).getPort();
//    MemberVM newServer = cluster.startServerVM(3, s -> s.withConnectionToLocator(locatorPort)
//        .withCredential("clusterManage", "clusterManage"));
  }
}
