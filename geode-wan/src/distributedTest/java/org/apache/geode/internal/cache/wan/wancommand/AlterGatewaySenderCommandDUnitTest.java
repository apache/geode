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
import static org.apache.geode.distributed.ConfigurationProperties.NAME;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.cache.wan.GatewayEventFilter;
import org.apache.geode.cache.wan.GatewayQueueEvent;
import org.apache.geode.cache.wan.GatewaySender;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.rules.GfshCommandRule;

@Category({WanTest.class})
public class AlterGatewaySenderCommandDUnitTest {

  public static final String CREATE =
      "create gateway-sender --id=sender1 --remote-distributed-system-id=2";
  public static final String CREATE_PARALLEL =
      "create gateway-sender --id=sender1P --remote-distributed-system-id=2 --parallel=true";
  public static final String DESTROY = "destroy gateway-sender --id=sender1";
  public static final String DESTROY_PARALLEL = "destroy gateway-sender --id=sender1P";


  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @Rule
  public GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locatorSite1;
  private static MemberVM server1;
  private static MemberVM server2;
  private final IgnoredException exln = IgnoredException
      .addIgnoredException("could not get remote locator information for remote site");
  private final IgnoredException exln1 = IgnoredException
      .addIgnoredException("Connection reset");
  private final IgnoredException exln2 = IgnoredException
      .addIgnoredException("Broken pipe");
  private final IgnoredException exln3 = IgnoredException
      .addIgnoredException("Connection refused");
  private final IgnoredException exln4 = IgnoredException
      .addIgnoredException("Unexpected IOException");

  @BeforeClass
  public static void beforeClass() throws Exception {
    Properties props = new Properties();
    props.setProperty(NAME, "happylocator");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    locatorSite1 = clusterStartupRule.startLocatorVM(0, props);

    props.setProperty(NAME, "happyserver1");
    server1 = clusterStartupRule.startServerVM(1, props, locatorSite1.getPort());

    props.setProperty(NAME, "happyserver2");
    server2 = clusterStartupRule.startServerVM(2, props, locatorSite1.getPort());

    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 2);
    props.setProperty(NAME, "happyremotelocator");
    props.setProperty(REMOTE_LOCATORS, "localhost[" + locatorSite1.getPort() + "]");
    clusterStartupRule.startLocatorVM(3, props);
  }

  @Before
  public void before() throws Exception {
    gfsh.connectAndVerify(locatorSite1);
  }

  @After
  public void after() {
    gfsh.executeAndAssertThat("destroy region --name=parentRegion").statusIsSuccess();
    gfsh.executeAndAssertThat(DESTROY + " --if-exists").statusIsSuccess();
    gfsh.executeAndAssertThat(DESTROY_PARALLEL + " --if-exists").statusIsSuccess();
    exln.remove();
    exln1.remove();
    exln2.remove();
    exln3.remove();
    exln4.remove();
  }

  @Test
  public void testCreateSerialGatewaySenderWithDefault() throws Exception {
    gfsh.executeAndAssertThat(CREATE).statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting")
        .hasTableSection()
        .hasColumn("Message")
        .containsExactly("GatewaySender \"sender1\" created on \"happyserver1\"",
            "GatewaySender \"sender1\" created on \"happyserver2\"");

    gfsh.executeAndAssertThat("list gateways").statusIsSuccess()
        .containsOutput("sender1");

    gfsh.executeAndAssertThat("create region"
        + " --name=parentRegion"
        + " --type=PARTITION"
        + " --gateway-sender-id=sender1").statusIsSuccess();
    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(GatewaySender.DEFAULT_BATCH_SIZE);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(GatewaySender.DEFAULT_ALERT_THRESHOLD);
      assertThat(sender.getDispatcherThreads()).isEqualTo(GatewaySender.DEFAULT_DISPATCHER_THREADS);
    });
  }

  @Test
  public void testCreateSerialGatewaySenderAndAlterBatchSize() throws Exception {
    gfsh.executeAndAssertThat(CREATE).statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting")
        .hasTableSection()
        .hasColumn("Message")
        .containsExactly("GatewaySender \"sender1\" created on \"happyserver1\"",
            "GatewaySender \"sender1\" created on \"happyserver2\"");

    gfsh.executeAndAssertThat("list gateways").statusIsSuccess()
        .containsOutput("sender1");

    gfsh.executeAndAssertThat("create region"
        + " --name=parentRegion"
        + " --type=PARTITION"
        + " --gateway-sender-id=sender1").statusIsSuccess();

    gfsh.executeAndAssertThat(
        "alter gateway-sender --id=sender1 --batch-size=200 --alert-threshold=100")
        .statusIsSuccess();

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(200);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(100);
    });
  }

  @Test
  public void testCreateSerialGatewaySenderAndInvalidAlterBatchSize() throws Exception {
    gfsh.executeAndAssertThat(CREATE).statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting")
        .hasTableSection()
        .hasColumn("Message")
        .containsExactly("GatewaySender \"sender1\" created on \"happyserver1\"",
            "GatewaySender \"sender1\" created on \"happyserver2\"");

    gfsh.executeAndAssertThat("list gateways").statusIsSuccess()
        .containsOutput("sender1");

    gfsh.executeAndAssertThat("create region"
        + " --name=parentRegion"
        + " --type=PARTITION"
        + " --gateway-sender-id=sender1").statusIsSuccess();

    gfsh.executeAndAssertThat(
        "alter gateway-sender --id=sender1 --batch-size=-10 --alert-threshold=100")
        .statusIsError();

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(GatewaySender.DEFAULT_BATCH_SIZE);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(GatewaySender.DEFAULT_ALERT_THRESHOLD);
    });
  }

  @Test
  public void testCreateSerialGatewaySenderAndAlterBatchSizeCheckConfig() throws Exception {
    gfsh.executeAndAssertThat(CREATE).statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting")
        .hasTableSection()
        .hasColumn("Message")
        .containsExactly("GatewaySender \"sender1\" created on \"happyserver1\"",
            "GatewaySender \"sender1\" created on \"happyserver2\"");

    gfsh.executeAndAssertThat("list gateways").statusIsSuccess()
        .containsOutput("sender1");

    gfsh.executeAndAssertThat("create region"
        + " --name=parentRegion"
        + " --type=PARTITION"
        + " --gateway-sender-id=sender1").statusIsSuccess();

    gfsh.executeAndAssertThat(
        "alter gateway-sender --id=sender1 --batch-size=200 --alert-threshold=100")
        .statusIsSuccess();

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(200);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(100);
    });

    // verify that server1's event queue has the default value
    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(200);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(100);
    });

    locatorSite1.invoke(() -> {
      InternalLocator locator = ClusterStartupRule.getLocator();
      assertThat(locator).isNotNull();
      String xml = locator.getConfigurationPersistenceService().getConfiguration("cluster")
          .getCacheXmlContent();
      assertThat(xml).contains("batch-size=\"200\"");
      assertThat(xml).contains("alert-threshold=\"100\"");
    });

  }

  @Test
  public void testCreateSerialGatewaySenderAndChangeGroupTransaction() throws Exception {
    gfsh.executeAndAssertThat(CREATE).statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting")
        .hasTableSection()
        .hasColumn("Message")
        .containsExactly("GatewaySender \"sender1\" created on \"happyserver1\"",
            "GatewaySender \"sender1\" created on \"happyserver2\"");

    gfsh.executeAndAssertThat("list gateways").statusIsSuccess()
        .containsOutput("sender1");

    gfsh.executeAndAssertThat("create region"
        + " --name=parentRegion"
        + " --type=PARTITION"
        + " --gateway-sender-id=sender1").statusIsSuccess();

    gfsh.executeAndAssertThat("alter gateway-sender --id=sender1 --group-transaction-events=true")
        .statusIsError()
        .containsOutput("alter-gateway-sender cannot be performed for --group-transaction-events");
  }


  @Test
  public void testCreateSerialGatewaySenderAndAlterBatchSizeServerDown() throws Exception {
    gfsh.executeAndAssertThat(CREATE).statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting")
        .hasTableSection()
        .hasColumn("Message")
        .containsExactly("GatewaySender \"sender1\" created on \"happyserver1\"",
            "GatewaySender \"sender1\" created on \"happyserver2\"");

    gfsh.executeAndAssertThat("list gateways").statusIsSuccess()
        .containsOutput("sender1");

    gfsh.executeAndAssertThat("create region"
        + " --name=parentRegion"
        + " --type=PARTITION"
        + " --gateway-sender-id=sender1").statusIsSuccess();

    server1.stop(false);

    gfsh.executeAndAssertThat(
        "alter gateway-sender --id=sender1 --batch-size=200 --alert-threshold=100")
        .statusIsSuccess();

    Properties props = new Properties();
    props.setProperty(NAME, "happyserver1");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, "" + 1);
    server1 = clusterStartupRule.startServerVM(1, props, locatorSite1.getPort());

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(200);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(100);
    });

  }

  @Test
  public void testCreateSerialGatewaySenderAndAlterEventFiters() throws Exception {
    gfsh.executeAndAssertThat(CREATE).statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting")
        .hasTableSection()
        .hasColumn("Message")
        .containsExactly("GatewaySender \"sender1\" created on \"happyserver1\"",
            "GatewaySender \"sender1\" created on \"happyserver2\"");

    gfsh.executeAndAssertThat("list gateways").statusIsSuccess()
        .containsOutput("sender1");

    gfsh.executeAndAssertThat("create region"
        + " --name=parentRegion"
        + " --type=PARTITION"
        + " --gateway-sender-id=sender1").statusIsSuccess();

    gfsh.executeAndAssertThat(
        "alter gateway-sender --id=sender1 --batch-size=200 --alert-threshold=100 --gateway-event-filter="
            + MyGatewayEventFilter.class.getName())
        .statusIsSuccess();

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(200);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(100);
      assertThat(sender.getGatewayEventFilters().get(0).beforeEnqueue(null)).isTrue();
      assertThat(sender.getGatewayEventFilters().size()).isEqualTo(1);

    });
    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(200);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(100);
      assertThat(sender.getGatewayEventFilters().get(0).beforeEnqueue(null)).isTrue();
      assertThat(sender.getGatewayEventFilters().size()).isEqualTo(1);
    });

    gfsh.executeAndAssertThat(
        "alter gateway-sender --id=sender1 --batch-size=2000 --alert-threshold=1000")
        .statusIsSuccess();

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(2000);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(1000);
      assertThat(sender.getGatewayEventFilters().get(0).beforeEnqueue(null)).isTrue();
      assertThat(sender.getGatewayEventFilters().size()).isEqualTo(1);

    });
    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(2000);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(1000);
      assertThat(sender.getGatewayEventFilters().get(0).beforeEnqueue(null)).isTrue();
      assertThat(sender.getGatewayEventFilters().size()).isEqualTo(1);
    });

  }

  @Test
  public void testCreateSerialGatewaySenderAndAlterEventFitersAndRemove() throws Exception {
    gfsh.executeAndAssertThat(CREATE).statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting")
        .hasTableSection()
        .hasColumn("Message")
        .containsExactly("GatewaySender \"sender1\" created on \"happyserver1\"",
            "GatewaySender \"sender1\" created on \"happyserver2\"");

    gfsh.executeAndAssertThat("list gateways").statusIsSuccess()
        .containsOutput("sender1");

    gfsh.executeAndAssertThat("create region"
        + " --name=parentRegion"
        + " --type=PARTITION"
        + " --gateway-sender-id=sender1").statusIsSuccess();

    gfsh.executeAndAssertThat(
        "alter gateway-sender --id=sender1 --batch-size=200 --alert-threshold=100 --gateway-event-filter="
            + MyGatewayEventFilter.class.getName())
        .statusIsSuccess();

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(200);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(100);
      assertThat(sender.getGatewayEventFilters().get(0).beforeEnqueue(null)).isTrue();
      assertThat(sender.getGatewayEventFilters().size()).isEqualTo(1);

    });
    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(200);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(100);
      assertThat(sender.getGatewayEventFilters().get(0).beforeEnqueue(null)).isTrue();
      assertThat(sender.getGatewayEventFilters().size()).isEqualTo(1);
    });

    gfsh.executeAndAssertThat(
        "alter gateway-sender --id=sender1 --batch-size=111 --alert-threshold=55 --gateway-event-filter")
        .statusIsSuccess();

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(111);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(55);
      assertThat(sender.getGatewayEventFilters().isEmpty()).isTrue();

    });
    server2.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1");
      assertThat(sender.getBatchSize()).isEqualTo(111);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(55);
      assertThat(sender.getGatewayEventFilters().isEmpty()).isTrue();
    });
  }


  @Test
  public void testCreateParallelGatewaySenderAndAlterBatchSize() throws Exception {
    gfsh.executeAndAssertThat(CREATE_PARALLEL).statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting")
        .hasTableSection()
        .hasColumn("Message")
        .containsExactly("GatewaySender \"sender1P\" created on \"happyserver1\"",
            "GatewaySender \"sender1P\" created on \"happyserver2\"");

    gfsh.executeAndAssertThat("list gateways").statusIsSuccess()
        .containsOutput("sender1P");

    gfsh.executeAndAssertThat("create region"
        + " --name=parentRegion"
        + " --type=PARTITION"
        + " --gateway-sender-id=sender1P").statusIsSuccess();

    gfsh.executeAndAssertThat(
        "alter gateway-sender --id=sender1P --batch-size=200 --alert-threshold=100")
        .statusIsSuccess();

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1P");
      assertThat(sender.getBatchSize()).isEqualTo(200);
      assertThat(sender.getBatchTimeInterval())
          .isEqualTo(GatewaySender.DEFAULT_BATCH_TIME_INTERVAL);
      assertThat(sender.getAlertThreshold()).isEqualTo(100);
    });
  }

  @Test
  public void testCreateParallelGatewaySenderAndChangeGroupTransaction() throws Exception {
    gfsh.executeAndAssertThat(CREATE_PARALLEL).statusIsSuccess()
        .doesNotContainOutput("Did not complete waiting")
        .hasTableSection()
        .hasColumn("Message")
        .containsExactly("GatewaySender \"sender1P\" created on \"happyserver1\"",
            "GatewaySender \"sender1P\" created on \"happyserver2\"");

    gfsh.executeAndAssertThat("list gateways").statusIsSuccess()
        .containsOutput("sender1P");

    gfsh.executeAndAssertThat("create region"
        + " --name=parentRegion"
        + " --type=PARTITION"
        + " --gateway-sender-id=sender1P").statusIsSuccess();

    gfsh.executeAndAssertThat("alter gateway-sender --id=sender1P --group-transaction-events=true")
        .statusIsSuccess();

    // verify that server1's event queue has the default value
    server1.invoke(() -> {
      InternalCache cache = ClusterStartupRule.getCache();
      GatewaySender sender = cache.getGatewaySender("sender1P");
      assertThat(sender.mustGroupTransactionEvents()).isTrue();
    });
  }

  public static class MyGatewayEventFilter implements GatewayEventFilter {
    @Override
    public void afterAcknowledgement(GatewayQueueEvent event) {}

    @Override
    public boolean beforeEnqueue(GatewayQueueEvent event) {
      return true;
    }

    @Override
    public boolean beforeTransmit(GatewayQueueEvent event) {
      return true;
    }

  }
}
