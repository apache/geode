package org.apache.geode.cache.client.internal;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import org.apache.geode.cache.Region;
import org.apache.geode.cache.RegionShortcut;
import org.apache.geode.cache.client.ClientCache;
import org.apache.geode.cache.client.ClientCacheFactory;
import org.apache.geode.cache.client.ClientRegionShortcut;
import org.apache.geode.test.dunit.rules.ClusterStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;

public class BasicClientProxyRegionOperationsDistributeTest {

  private static int locator1Port;

  private  ClientCache clientCache;
  private Region<String, String> region;

  @ClassRule
  public static ClusterStartupRule clusterStartupRule = new ClusterStartupRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    final MemberVM locator = clusterStartupRule.startLocatorVM(0, new Properties());
    locator1Port = locator.getPort();
    final MemberVM server1 = clusterStartupRule.startServerVM(1, locator1Port);
    server1.invoke(() -> {
      ClusterStartupRule.getCache().createRegionFactory(RegionShortcut.REPLICATE).create("region");
    });
  }

  @Before
  public void before() {
    clientCache = createClientCache(locator1Port);
    region =
        clientCache
            .<String, String>createClientRegionFactory(ClientRegionShortcut.PROXY)
            .create("region");
  }

  @After
  public void afterClass() {
    clientCache.close();
  }

  @Test
  public void getWithoutCallbackReturnsEntry() {
    region.put("getWithoutCallbackReturnsEntry", "yes");
    assertThat(region.get("getWithoutCallbackReturnsEntry")).isEqualTo("yes");
  }

  @Test
  public void getWithoutCallbackReturnsNullWhenDoesntExist() {
    assertThat(region.get("getWithoutCallbackReturnsNullWhenDoesntExist")).isNull();
  }

  @Test
  public void getWithCallbackReturnsEntry() {
    region.put("getWithCallbackReturnsEntry", "yes");
    assertThat(region.get("getWithCallbackReturnsEntry", "callback")).isEqualTo("yes");
  }

  @Test
  public void getWithCallbackReturnsNullWhenDoesntExist() {
    assertThat(region.get("getWithCallbackReturnsNullWhenDoesntExist", "callback")).isNull();
  }

  private static ClientCache createClientCache(int locator1Port) {
    ClientCacheFactory ccf = new ClientCacheFactory();
    ccf.addPoolLocator("localhost", locator1Port);
    ccf.setPoolSubscriptionEnabled(true);
    return ccf.create();
  }

}
