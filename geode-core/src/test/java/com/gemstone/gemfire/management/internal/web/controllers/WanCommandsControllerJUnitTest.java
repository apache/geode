/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.management.internal.web.controllers;

import static com.gemstone.gemfire.management.internal.cli.i18n.CliStrings.*;
import static junitparams.JUnitParamsRunner.$;
import static org.assertj.core.api.Assertions.*;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.gemstone.gemfire.test.junit.categories.UnitTest;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * Unit tests for WanCommandsController. 
 * 
 * Introduced for GEODE-213 "JMX -http manager treats "start gateway-sender" as "start gateway-receiver"
 *  
 * @author Kirk Lund
 */
@SuppressWarnings("unused")
@Category(UnitTest.class)
@RunWith(JUnitParamsRunner.class)
public class WanCommandsControllerJUnitTest {

  private TestableWanCommandsController wanCommandsController;
  
  @Before
  public void setUp() {
    this.wanCommandsController = new TestableWanCommandsController();
  }
  
  @Test
  public void shouldDefineStartGatewayReceiverCommandWithNulls() {
    this.wanCommandsController.startGatewaySender(null, null, null);
    
    assertThat(this.wanCommandsController.testableCommand).contains("--"+START_GATEWAYSENDER__ID+"="+null);
    assertThat(this.wanCommandsController.testableCommand).contains(START_GATEWAYSENDER);
    assertThat(this.wanCommandsController.testableCommand).doesNotContain(START_GATEWAYRECEIVER__GROUP);
    assertThat(this.wanCommandsController.testableCommand).doesNotContain(START_GATEWAYRECEIVER__MEMBER);
  }
  
  @Test
  @Parameters(method = "getParametersWithGroupsAndMembers")
  public void shouldDefineStartGatewayReceiverCommandWithGroupsAndMembers(final String gatewaySenderId, final String[] groups, final String[] members, final boolean containsGroups, final boolean containsMembers) {
    this.wanCommandsController.startGatewaySender(gatewaySenderId, groups, members);
    
    assertThat(this.wanCommandsController.testableCommand).contains(START_GATEWAYSENDER);
    assertThat(this.wanCommandsController.testableCommand).contains("--"+START_GATEWAYSENDER__ID+"="+gatewaySenderId);
    assertThat(this.wanCommandsController.testableCommand.contains(START_GATEWAYRECEIVER__GROUP)).isEqualTo(containsGroups);
    assertThat(this.wanCommandsController.testableCommand.contains(START_GATEWAYRECEIVER__MEMBER)).isEqualTo(containsMembers);
    if (containsGroups) {
      assertThat(this.wanCommandsController.testableCommand).contains("--"+START_GATEWAYRECEIVER__GROUP+"="+groups[0]);
    }
    if (containsMembers) {
      assertThat(this.wanCommandsController.testableCommand).contains("--"+START_GATEWAYRECEIVER__MEMBER+"="+members[0]);
    }
  }
  
  /**
   * Tests null gatewaySenderId.
   * 
   * This test verifies that the class under test allows null value which 
   * would only be rejected at runtime by the overall framework and is tested 
   * within an integration test.
   *
   * see WanCommandGatewaySenderStartDUnitTest
   */
  @Test
  public void shouldDefineStartGatewayReceiverCommandWithNullGatewaySenderId() {
    this.wanCommandsController.startGatewaySender(null, null, null);
    
    assertThat(this.wanCommandsController.testableCommand).contains(START_GATEWAYSENDER);
    assertThat(this.wanCommandsController.testableCommand).contains("--"+START_GATEWAYSENDER__ID+"="+null);
  }
  
  /**
   * Tests empty gatewaySenderId.
   * 
   * This test verifies that the class under test allows empty value which 
   * would only be rejected at runtime by the overall framework and is tested 
   * within an integration test.
   *
   * see WanCommandGatewaySenderStartDUnitTest
   */
  @Test
  public void shouldDefineStartGatewayReceiverCommandWithEmptyGatewaySenderId() {
    this.wanCommandsController.startGatewaySender("", null, null);
    
    assertThat(this.wanCommandsController.testableCommand).contains(START_GATEWAYSENDER);
    assertThat(this.wanCommandsController.testableCommand).contains("--"+START_GATEWAYSENDER__ID+"="+"");
  }
  
  private static final Object[] getParametersWithGroupsAndMembers() {
    return $(
        new Object[] { "sender1", new String[] { }, new String[] { }, false, false },
        new Object[] { "sender2", new String[] { "group1" }, new String[] { }, true, false },
        new Object[] { "sender3", new String[] { "group1", "group2" }, new String[] { }, true, false },
        new Object[] { "sender4", new String[] { }, new String[] { "member1" }, false, true },
        new Object[] { "sender5", new String[] { }, new String[] { "member1", "member2" }, false, true },
        new Object[] { "sender6", new String[] { "group1" }, new String[] { "member1" }, true, true },
        new Object[] { "sender7", new String[] { "group1", "group2" }, new String[] { "member1", "member2" }, true, true }
    );
  }
  
  /**
   * It would be ideal to refactor AbstractCommandsController such that 
   * defining of command strings and submitting them for execution are
   * performed by two different classes. Then we could mock the executor
   * class while testing just the command string definition class.
   */
  public static class TestableWanCommandsController extends WanCommandsController {
    protected String testableCommand;
    
    @Override
    protected String processCommand(final String command) {
      this.testableCommand = command;
      return null; // do nothing
    }
  }
}
