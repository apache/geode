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
package org.apache.geode.management.internal.cli;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.test.junit.rules.GfshParserRule;

public class OptionAliasesParsingTest {

  private String buffer;

  @ClassRule
  public static GfshParserRule parser = new GfshParserRule();

  @Test
  public void startLocator() {
    buffer = "start locator --name=locator1 --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void startServer() {
    buffer = "start server --name=server1 --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void exportConfig() {
    buffer = "export config --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void createRegion() {
    buffer = "create region --name=region1 --type=REPLICATE_PERSISTENT --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void alterRegion() {
    buffer = "alter region --name=region1 --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void deploy() {
    buffer = "deploy --jars=j1,j2 --groups=g1,g2";
    validateParsedResults(false, true, true);
  }

  @Test
  public void undeploy() {
    buffer = "undeploy --jars=j1,j2 --groups=g1,g2";
    validateParsedResults(false, true, true);
  }

  @Test
  public void listDeployed() {
    buffer = "list deployed --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void createDiskStore() {
    buffer = "create disk-store --name=ds1 --dir=dir1 --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void compactDiskStore() {
    buffer = "compact disk-store --name=ds1 --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void destroyDiskStore() {
    buffer = "destroy disk-store --name=ds1 --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void listDurableCQs() {
    buffer = "list durable-cqs --durable-client-id=id1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void showSubscriptionQueueSize() {
    buffer = "show subscription-queue-size --durable-client-id=id1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void closeDurableCQs() {
    buffer =
        "close durable-cq --durable-client-id=id1 --durable-cq-name=cq1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void closeDurableClient() {
    buffer = "close durable-client --durable-client-id=id1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void exportLogs() {
    buffer = "export logs --dir=/temp --groups=g1,g2 --members=m1,m2";
    validateParsedResults(true, true);
  }

  @Test
  public void executeFunction() {
    buffer = "execute function --id=function1 --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void destroyFunction() {
    buffer = "destroy function --id=function1 --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void listFunctions() {
    buffer = "list functions --groups=g1,g2 --members=m1,m2";
    validateParsedResults(true, true);
  }

  @Test
  public void createIndex() {
    buffer =
        "create index --name=index1 --expression=expression1.id --region=region1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void destroyIndex() {
    buffer = "destroy index --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void createDefinedIndexes() {
    buffer = "create defined indexes --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void gc() {
    buffer = "gc --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void netstat() {
    buffer = "netstat --members=m1,m2";
    validateParsedResults(true, false);
  }

  @Test
  public void exportStackTraces() {
    buffer = "export stack-traces --file=file1.txt --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void changeLogLevel() {
    buffer = "change loglevel --loglevel=severe --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void createAsyncEventQueue() {
    buffer = "create async-event-queue --id=id1 --listener=listener1 --groups=g1,g2";
    validateParsedResults(false, true);
  }

  @Test
  public void listRegions() {
    buffer = "list regions --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void createGatewaySender() {
    buffer =
        "create gateway-sender --id=id1 --remote-distributed-system-id=2 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void startGatewaySender() {
    buffer = "start gateway-sender --id=sender1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void pauseGatewaySender() {
    buffer = "pause gateway-sender --id=sender1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void resumeGatewaySender() {
    buffer = "resume gateway-sender --id=sender1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void stopGatewaySender() {
    buffer = "stop gateway-sender --id=sender1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void createGatewayReceiver() {
    buffer = "create gateway-receiver --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void startGatewayReceiver() {
    buffer = "start gateway-receiver --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void stopGatewayReceiver() {
    buffer = "stop gateway-receiver --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void listGateways() {
    buffer = "list gateways --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void statusGatewaySender() {
    buffer = "status gateway-sender --id=sender1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void statusGatewayReceiver() {
    buffer = "status gateway-receiver --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void destroyGatewaySender() {
    buffer = "destroy gateway-sender --id=sender1 --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  @Test
  public void alterRuntime() {
    buffer = "alter runtime --members=m1,m2 --groups=g1,g2";
    validateParsedResults(true, true);
  }

  private void validateParsedResults(boolean canHaveMembers, boolean canHaveGroups) {
    GfshParseResult result = parser.parse(buffer);
    if (canHaveMembers) {
      assertThat(result.getParamValueAsString("member")).isEqualTo("m1,m2");
    }
    if (canHaveGroups) {
      assertThat(result.getParamValueAsString("group")).isEqualTo("g1,g2");
    }
  }

  private void validateParsedResults(boolean canHaveMembers, boolean canHaveGroups,
      boolean canHaveJars) {
    GfshParseResult result = parser.parse(buffer);
    validateParsedResults(canHaveMembers, canHaveGroups);
    if (canHaveJars) {
      assertThat(result.getParamValueAsString("jar")).isEqualTo("j1,j2");
    }
  }

  /**
   * This characterizes the current behavior but it may be incorrect.
   */
  @Test
  public void gc_memberWithCommas() {
    buffer = "gc --member=m1,m2";
    GfshParseResult result = parser.parse(buffer);
    assertThat(result.getParamValueAsString("member")).isEqualTo("m1,m2");
  }

  @Test
  public void gc_onlySupportsMember_returnsNull() {
    buffer = "gc --members=m1,m2";
    assertThat(parser.parse(buffer)).isNull();
  }

  @Test
  public void destroyFunction_memberWithCommas() {
    buffer = "destroy function --id=function1 --member=m1,m2";
    GfshParseResult result = parser.parse(buffer);
    assertThat(result.getParamValueAsString("member")).isEqualTo("m1,m2");
  }

  @Test
  public void destroyFunction_onlySupportsMember_returnsNull() {
    buffer = "destroy function --id=function1 --members=m1,m2";
    assertThat(parser.parse(buffer)).isNull();
  }

  @Test
  public void memberAndMembersReturnsNull() {
    buffer =
        "create region --name=region1 --type=REPLICATE_PERSISTENT --members=m1,m2 --member=m4,m5";
    assertThat(parser.parse(buffer)).isNull();
  }

  @Test
  public void groupAndGroupsReturnsNull() {
    buffer =
        "create region --name=region1 --type=REPLICATE_PERSISTENT --groups=g1,g2 --group=g4,g5";
    assertThat(parser.parse(buffer)).isNull();
  }
}
