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

package org.apache.geode.internal.cache.wan.wancommand;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;

import java.util.Properties;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;

@Category(DistributedTest.class)
public class DestroyGatewaySenderCommandDUnitTest {

  @Rule
  public LocatorServerStartupRule locatorServerStartupRule = new LocatorServerStartupRule();

  @Rule
  public GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  private MemberVM locator;

  @Before
  public void before() throws Exception {
    Properties props = new Properties();
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locator = locatorServerStartupRule.startLocatorVM(0, props);
    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testDestroyGatewaySender_NotCreatedSender() throws Exception {
    locatorServerStartupRule.startServerVM(1, locator.getPort());
    locatorServerStartupRule.startServerVM(2, locator.getPort());

    // destroy a sender that does not exist
    gfsh.executeAndAssertThat("destroy gateway-sender --id=ln").statusIsError()
        .tableHasColumnWithValuesContaining("Status", "ERROR", "ERROR");
  }
}
