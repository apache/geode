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

package org.apache.geode.management.internal.cli.commands;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;

import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;
import org.apache.geode.test.junit.rules.ServerStarterRule;


@Category(IntegrationTest.class)
public class PutCommandIntegrationTest {

  private static ServerStarterRule server =
      new ServerStarterRule().withJMXManager().withRegion(RegionShortcut.REPLICATE, "testRegion");

  private static GfshShellConnectionRule gfsh =
      new GfshShellConnectionRule(server::getJmxPort, GfshShellConnectionRule.PortType.jmxManager);

  @ClassRule
  public static RuleChain chain = RuleChain.outerRule(server).around(gfsh);


  @Test
  public void putWithoutSlash() throws Exception {
    gfsh.executeAndVerifyCommand("put --region=testRegion --key=key1 --value=value1");
  }


  @Test
  public void putWithSlash() throws Exception {
    gfsh.executeAndVerifyCommand("put --region=/testRegion --key=key1 --value=value1");
  }
}
