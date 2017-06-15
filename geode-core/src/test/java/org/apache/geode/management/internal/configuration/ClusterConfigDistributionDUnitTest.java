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
package org.apache.geode.management.internal.configuration;

import static org.apache.commons.io.FileUtils.writeByteArrayToFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.Region;
import org.apache.geode.cache.asyncqueue.AsyncEventQueue;
import org.apache.geode.cache.query.Index;
import org.apache.geode.internal.ClassBuilder;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.util.CommandStringBuilder;
import org.apache.geode.test.dunit.rules.GfshShellConnectionRule;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

@Category(DistributedTest.class)
public class ClusterConfigDistributionDUnitTest {

  private static final String REPLICATE_REGION = "ReplicateRegion1";
  private static final String PARTITION_REGION = "PartitionRegion1";
  private static final String INDEX1 = "ID1";
  private static final String INDEX2 = "ID2";
  private static final String AsyncEventQueue1 = "Q1";

  private MemberVM locator;

  @Rule
  public LocatorServerStartupRule lsRule = new LocatorServerStartupRule();
  @Rule
  public GfshShellConnectionRule gfshConnector = new GfshShellConnectionRule();

  @Before
  public void before() throws Exception {
    locator = lsRule.startLocatorVM(0);
    gfshConnector.connect(locator);
    assertThat(gfshConnector.isConnected()).isTrue();

    // start a server so that we can execute data commands that requires at least a server running
    lsRule.startServerVM(1, locator.getPort());
  }

  @Test
  public void testIndexAndAsyncEventQueueCommands() throws Exception {
    final String DESTROY_REGION = "regionToBeDestroyed";

    gfshConnector
        .executeAndVerifyCommand("create region --name=" + REPLICATE_REGION + " --type=REPLICATE");
    gfshConnector
        .executeAndVerifyCommand("create region --name=" + PARTITION_REGION + " --type=PARTITION");
    gfshConnector
        .executeAndVerifyCommand("create region --name=" + DESTROY_REGION + " --type=REPLICATE");

    gfshConnector.executeAndVerifyCommand(
        "create index --name=" + INDEX1 + " --expression=AAPL --region=" + REPLICATE_REGION);
    gfshConnector.executeAndVerifyCommand(
        "create index --name=" + INDEX2 + " --expression=VMW --region=" + PARTITION_REGION);


    String asyncEventQueueJarPath = createAsyncEventQueueJar();
    gfshConnector.executeAndVerifyCommand("deploy --jar=" + asyncEventQueueJarPath);

    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CREATE_ASYNC_EVENT_QUEUE);
    csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__ID, AsyncEventQueue1);
    csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__LISTENER,
        "com.qcdunit.QueueCommandsDUnitTestListener");
    csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__DISK_STORE, null);
    csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__BATCH_SIZE, "1000");
    csb.addOptionWithValueCheck(CliStrings.GROUP, null);
    csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__PERSISTENT, "false");
    csb.addOptionWithValueCheck(CliStrings.CREATE_ASYNC_EVENT_QUEUE__MAXIMUM_QUEUE_MEMORY, "1000");
    gfshConnector.executeAndVerifyCommand(csb.getCommandString());

    gfshConnector.executeAndVerifyCommand("destroy region --name=" + DESTROY_REGION);

    gfshConnector.executeAndVerifyCommand(
        "destroy index --name=" + INDEX2 + " --region=" + PARTITION_REGION);
    gfshConnector.executeAndVerifyCommand("alter runtime --copy-on-read=true");

    // Start a new member which receives the shared configuration
    // Verify the config creation on this member

    MemberVM server = lsRule.startServerVM(2, new Properties(), locator.getPort());

    server.invoke(() -> {
      Cache cache = LocatorServerStartupRule.serverStarter.getCache();
      assertNotNull(cache);
      assertTrue(cache.getCopyOnRead());

      Region region1 = cache.getRegion(REPLICATE_REGION);
      assertNotNull(region1);
      Region region2 = cache.getRegion(PARTITION_REGION);
      assertNotNull(region2);

      Region region3 = cache.getRegion(DESTROY_REGION);
      assertNull(region3);

      // Index verification
      Index index1 = cache.getQueryService().getIndex(region1, INDEX1);
      assertNotNull(index1);
      assertNull(cache.getQueryService().getIndex(region2, INDEX2));

      // ASYNC-EVENT-QUEUE verification
      AsyncEventQueue aeq = cache.getAsyncEventQueue(AsyncEventQueue1);
      assertNotNull(aeq);
      assertFalse(aeq.isPersistent());
      assertTrue(aeq.getBatchSize() == 1000);
      assertTrue(aeq.getMaximumQueueMemory() == 1000);
    });
  }

  private String createAsyncEventQueueJar() throws IOException {
    String queueCommandsJarName = this.lsRule.getTempFolder().getRoot().getCanonicalPath()
        + File.separator + "testEndToEndSC-QueueCommands.jar";
    final File jarFile = new File(queueCommandsJarName);

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

    writeByteArrayToFile(jarFile, jarBytes);
    return queueCommandsJarName;
  }

  @Test
  public void testConfigurePDX() throws Exception {
    CommandStringBuilder csb = new CommandStringBuilder(CliStrings.CONFIGURE_PDX);
    csb.addOptionWithValueCheck(CliStrings.CONFIGURE_PDX__AUTO__SERIALIZER__CLASSES, "com.foo.*");
    csb.addOptionWithValueCheck(CliStrings.CONFIGURE_PDX__IGNORE__UNREAD_FIELDS, "true");
    csb.addOptionWithValueCheck(CliStrings.CONFIGURE_PDX__READ__SERIALIZED, "true");

    String message = gfshConnector.execute(csb.getCommandString());
    assertThat(message).contains(CliStrings.CONFIGURE_PDX__NORMAL__MEMBERS__WARNING);
  }
}
