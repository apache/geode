package com.gemstone.gemfire.management;

import static com.gemstone.gemfire.distributed.ConfigurationProperties.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.RegionShortcut;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.Invoke;
import com.gemstone.gemfire.test.dunit.RMIException;
import com.gemstone.gemfire.test.dunit.VM;
import com.gemstone.gemfire.test.dunit.cache.internal.JUnit4CacheTestCase;
import com.gemstone.gemfire.test.junit.categories.DistributedTest;
import com.gemstone.gemfire.test.junit.categories.SecurityTest;

@Category({ DistributedTest.class, SecurityTest.class })
public class RegionCreateDestroyDUnitTest extends JUnit4CacheTestCase {

  private static final String GOOD_REGION_NAME = "Good-Region";
  private static final String BAD_REGION_NAME = "Bad@Region";
  private static final String RESERVED_REGION_NAME = "__ReservedRegion";

  protected VM client1 = null;
  protected VM client2 = null;
  protected VM client3 = null;
  protected int serverPort;

  @Before
  public void before() throws Exception {
    final Host host = Host.getHost(0);
    this.client1 = host.getVM(1);
    this.client2 = host.getVM(2);
    this.client3 = host.getVM(3);

    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");

    getSystem(props);

  }

  private void startServer(final Cache cache) throws IOException {
    CacheServer server1 = cache.addCacheServer();
    server1.setPort(0);
    server1.start();

    this.serverPort = server1.getPort();
  }

  @Override
  public void preTearDownCacheTestCase() throws Exception {
    Invoke.invokeInEveryVM(() -> closeCache());
    closeCache();
  }

  protected Properties createClientProperties() {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, "");
    props.setProperty(SECURITY_LOG_LEVEL, "finest");
    return props;
  }

  @Test
  public void testCreateDestroyValidRegion() throws InterruptedException {
    Cache serverCache = getCache();
    serverCache.createRegionFactory(RegionShortcut.REPLICATE).create(GOOD_REGION_NAME);

    try {
      startServer(serverCache);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    client1.invoke(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties()).setPoolSubscriptionEnabled(true)
                                                                          .addPoolServer("localhost", serverPort)
                                                                          .create();
      Region region = cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(GOOD_REGION_NAME);
      region.destroyRegion();
      assertThat(region.isDestroyed()).isTrue();
    });
  }

  @Test
  public void testCreateDestroyInvalidRegion() throws InterruptedException {
    Cache serverCache = getCache();
    try {
      serverCache.createRegionFactory(RegionShortcut.REPLICATE).create(BAD_REGION_NAME);
    } catch (IllegalArgumentException iae) {
      assertEquals("Region names may only be alphanumeric and may contain hyphens or underscores: Bad@Region", iae.getMessage());
    }

    try {
      startServer(serverCache);
    } catch (IOException e) {
      fail(e.getMessage());
    }
    client1.invoke(() -> {
      ClientCache cache = new ClientCacheFactory(createClientProperties()).setPoolSubscriptionEnabled(true)
                                                                          .addPoolServer("localhost", serverPort)
                                                                          .create();
      try {
        cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(BAD_REGION_NAME);
        fail("Should have thrown an IllegalArgumentException");
      } catch (IllegalArgumentException iae) {
        assertEquals("Region names may only be alphanumeric and may contain hyphens or underscores: Bad@Region", iae.getMessage());
      }
    });
  }

  @Test
  public void testCreateDestroyReservedRegion() throws InterruptedException {
    Cache serverCache = getCache();
    try {
      serverCache.createRegionFactory(RegionShortcut.REPLICATE).create(RESERVED_REGION_NAME);
      fail("Should have thrown an IllegalArgumentException");
    } catch (IllegalArgumentException arg) {
      assertEquals("Region names may not begin with a double-underscore: __ReservedRegion", arg.getMessage());
    }
    try {
      startServer(serverCache);
    } catch (IOException e) {
      fail(e.getMessage());
    }

    try {
      client1.invoke(() -> {
        ClientCache cache = new ClientCacheFactory(createClientProperties()).setPoolSubscriptionEnabled(true)
                                                                            .addPoolServer("localhost", serverPort)
                                                                            .create();
        try {
          cache.createClientRegionFactory(ClientRegionShortcut.PROXY).create(RESERVED_REGION_NAME);
          fail("Should have thrown an IllegalArgumentException");
        } catch (IllegalArgumentException e) {
          assertEquals("Region names may not begin with a double-underscore: __ReservedRegion", e.getMessage());
        }
      });
    } catch (RMIException rmi) {
      rmi.getCause();
    }

  }
}
