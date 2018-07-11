package org.apache.geode.management.internal.cli.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientRegionFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.cache.client.PoolManager;
import org.apache.geode.cache.client.internal.PoolImpl;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.ClientVM;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.rules.GfshCommandRule;

public class ListClientCommandDUnitTest {
  @ClassRule
  public static ClusterStartupRule cluster = new ClusterStartupRule(6);

  @ClassRule
  public static GfshCommandRule gfsh = new GfshCommandRule();

  private static MemberVM locator, server1, server2;

  private static ClientVM client1, client2, client3;

  @BeforeClass
  public static void setup() throws Exception {
    locator = cluster.startLocatorVM(0);
    int locatorPort = locator.getPort();
    server1 = cluster.startServerVM(1,
        r -> r.withRegion(RegionShortcut.REPLICATE, "stocks").withConnectionToLocator(locatorPort));
    server2 = cluster.startServerVM(2,
        r -> r.withRegion(RegionShortcut.REPLICATE, "stocks").withConnectionToLocator(locatorPort));

    gfsh.connectAndVerify(locator);
  }

  @Test
  public void testTwoClientsConnectToOneServer() throws Exception {
    int server1port = server1.getPort();
    Properties client1props = new Properties();
    client1props.setProperty("name", "client-1");
    client1 = cluster.startClientVM(3, client1props, cf -> {
      cf.addPoolServer("localhost", server1port);
      cf.setPoolSubscriptionEnabled(true);
    });
    Properties client2props = new Properties();
    client2props.setProperty("name", "client-2");
    client2 = cluster.startClientVM(4, client2props, cf -> {
      cf.addPoolServer("localhost", server1port);
      cf.setPoolSubscriptionEnabled(true);
    });

    MemberVM.invokeInEveryMember(() -> {
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<Object, Object> regionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.LOCAL)
              .setPoolName(clientCache.getDefaultPool().getName());
      Region<Object, Object> dataRegion = regionFactory.create("stocks");
      assertNotNull(dataRegion);
      dataRegion.put("k1", "v1");
      dataRegion.put("k2", "v2");
    }, client1, client2);

    locator.waitTillClientsAreReadyOnServers("server-1", server1port, 2);

    CommandResult result =
        gfsh.executeAndAssertThat("list clients").statusIsSuccess().getCommandResult();

    List<String> clientList =
        result.getColumnFromTableContent("Client Name / ID", "section1", "TableForClientList");
    assertThat(clientList).hasSize(2);
    try {
      assertThat(clientList.get(0)).contains("client-1");
      assertThat(clientList.get(1)).contains("client-2");
    } catch (AssertionError e) {
      assertThat(clientList.get(0)).contains("client-2");
      assertThat(clientList.get(1)).contains("client-1");
    }

    assertThat(
        result.getColumnFromTableContent("Server Name / ID", "section1", "TableForClientList"))
            .hasSize(2)
            .containsExactlyInAnyOrder("member=server-1,port=" + server1port,
                "member=server-1,port=" + server1port);

    // shutdown the clients
    cluster.stopMember(3);
    cluster.stopMember(4);
  }

  @Test
  public void oneClientConnectToTwoServers() throws Exception {
    int server1port = server1.getPort();
    int server2port = server2.getPort();
    Properties client1props = new Properties();
    client1props.setProperty("name", "client-1");
    client1 = cluster.startClientVM(3, client1props, cf -> {
      cf.addPoolServer("localhost", server1port);
      cf.setPoolSubscriptionEnabled(true);
    });

    client1.invoke(() -> {
      String poolName = "new_pool_" + System.currentTimeMillis();
      try {
        PoolImpl p = (PoolImpl) PoolManager.createFactory()
            .addServer("localhost", server2port).setThreadLocalConnections(true)
            .setMinConnections(1).setSubscriptionEnabled(true).setPingInterval(1)
            .setStatisticInterval(1).setMinConnections(1).setSubscriptionRedundancy(1)
            .create(poolName);
        assertNotNull(p);
      } catch (Exception eee) {
        System.err.println("Exception in creating pool " + poolName + "    Exception =="
            + ExceptionUtils.getStackTrace(eee));
      }

      // create the region
      ClientCache clientCache = ClusterStartupRule.getClientCache();
      ClientRegionFactory<Object, Object> regionFactory =
          clientCache.createClientRegionFactory(ClientRegionShortcut.LOCAL)
              .setPoolName(clientCache.getDefaultPool().getName());
      Region<Object, Object> dataRegion = regionFactory.create("stocks");
      assertNotNull(dataRegion);
      dataRegion.put("k1", "v1");
      dataRegion.put("k2", "v2");
    });

    locator.waitTillClientsAreReadyOnServers("server-1", server1port, 1);
    locator.waitTillClientsAreReadyOnServers("server-2", server2port, 1);

    CommandResult result =
        gfsh.executeAndAssertThat("list clients").statusIsSuccess().getCommandResult();

    List<String> clientList =
        result.getColumnFromTableContent("Client Name / ID", "section1", "TableForClientList");
    assertThat(clientList).hasSize(1);
    assertThat(clientList.get(0)).contains("client-1");

    List<String> serverList =
        result.getColumnFromTableContent("Server Name / ID", "section1", "TableForClientList");
    assertThat(serverList).hasSize(1);
    assertThat(serverList.get(0)).contains("server-1").contains("server-2");

    // shutdown the clients
    cluster.stopMember(3);
  }
}
