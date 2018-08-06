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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.compiler.ClassBuilder;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.GfshTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({GfshTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class ClusterConfigurationDUnitTest {
  @Rule
  public ClusterStartupRule startupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Parameterized.Parameter
  public boolean connectOverHttp;

  @Parameterized.Parameters
  public static Collection<Boolean> data() {
    return Arrays.asList(true, false);
  }

  // TODO mark GEODE-1606 resolved after
  @Test
  public void testStartServerAndExecuteCommands() throws Exception {
    MemberVM locator = startupRule.startLocatorVM(0, l -> l.withHttpService());
    if (connectOverHttp) {
      gfsh.connectAndVerify(locator.getHttpPort(), GfshCommandRule.PortType.http);
    } else {
      gfsh.connectAndVerify(locator);
    }

    MemberVM server1 = startupRule.startServerVM(1, locator.getPort());
    MemberVM server2 = startupRule.startServerVM(2, locator.getPort());

    // create regions, index and asyncEventQueue
    gfsh.executeAndAssertThat("create region --name=R1 --type=REPLICATE").statusIsSuccess();
    gfsh.executeAndAssertThat("create region --name=R2 --type=PARTITION").statusIsSuccess();
    gfsh.executeAndAssertThat("create index --name=ID1 --expression=AAPL --region=R1")
        .statusIsSuccess();
    createAsyncEventQueue("Q1");

    MemberVM server3 = startupRule.startServerVM(3, locator.getPort());

    gfsh.executeAndAssertThat("describe region --name=R1").statusIsSuccess();
    verifyContainsAllServerNames(gfsh.getGfshOutput(), server1.getName(), server2.getName(),
        server3.getName());

    gfsh.executeAndAssertThat("describe region --name=R2").statusIsSuccess();
    verifyContainsAllServerNames(gfsh.getGfshOutput(), server1.getName(), server2.getName(),
        server3.getName());

    gfsh.executeAndAssertThat("list indexes").statusIsSuccess();
    verifyContainsAllServerNames(gfsh.getGfshOutput(), server1.getName(), server2.getName(),
        server3.getName());

    gfsh.executeAndAssertThat("list async-event-queues").statusIsSuccess();
    verifyContainsAllServerNames(gfsh.getGfshOutput(), server1.getName(), server2.getName(),
        server3.getName());
  }

  private void verifyContainsAllServerNames(String result, String... serverNames) {
    for (String serverName : serverNames) {
      assertTrue(result.contains(serverName));
    }
  }

  private void createAsyncEventQueue(final String queueName) throws Exception {
    String queueCommandsJarName = "testEndToEndSC-QueueCommands.jar";
    final File jarFile = temporaryFolder.newFile(queueCommandsJarName);
    ClassBuilder classBuilder = new ClassBuilder();
    byte[] jarBytes =
        classBuilder.createJarFromClassContent("com/qcdunit/QueueCommandsDUnitTestListener",
            "package com.qcdunit;" + "import java.util.List; import java.util.Properties;"
                + "import org.apache.geode.internal.cache.xmlcache.Declarable2; import org.apache.geode.cache.asyncqueue.AsyncEvent;"
                + "import org.apache.geode.cache.asyncqueue.AsyncEventListener;"
                + "public class QueueCommandsDUnitTestListener implements Declarable2, AsyncEventListener {"
                + "Properties props;"
                + "public boolean processEvents(List<AsyncEvent> events) { return true; }"
                + "public void close() {}"
                + "public void init(final Properties props) {this.props = props;}"
                + "public Properties getConfig() {return this.props;}}");

    FileUtils.writeByteArrayToFile(jarFile, jarBytes);
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.DEPLOY);
    csb.addOption(CliStrings.JAR, jarFile.getAbsolutePath());
    gfsh.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();

    csb = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, queueName);
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER,
        "com.qcdunit.QueueCommandsDUnitTestListener");
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, "100");
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCHTIMEINTERVAL, "200");
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISPATCHERTHREADS, "4");
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ENABLEBATCHCONFLATION, "true");
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISKSYNCHRONOUS, "true");
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__FORWARD_EXPIRATION_DESTROY, "false");
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY, "1000");
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ORDERPOLICY,
        GatewaySender.OrderPolicy.KEY.toString());
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, "true");
    csb.addOption(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PARALLEL, "true");

    gfsh.executeAndAssertThat(csb.getCommandString()).statusIsSuccess();
  }
}
