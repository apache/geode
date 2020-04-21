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
package org.apache.geode.internal.cache;

import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.GROUPS;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.test.awaitility.GeodeAwaitility.await;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.server.CacheServer;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.DistributionAdvisee;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort.Keeper;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.logging.internal.log4j.api.LogService;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;

/**
 * Tests the GridAdvisor
 *
 * @since GemFire 5.7
 */

public class GridAdvisorDUnitTest extends JUnit4DistributedTestCase {
  private final Logger logger = LogService.getLogger();
  private static InternalCache cache;


  /**
   * Tests 2 controllers and 2 cache servers
   */
  @Test
  public void test2by2() {
    disconnectAllFromDS();

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);

    List<Keeper> freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPortKeepers(6);
    final Keeper keeper1 = freeTCPPorts.get(0);
    final int port1 = keeper1.getPort();
    final Keeper keeper2 = freeTCPPorts.get(1);
    final int port2 = keeper2.getPort();
    final Keeper bsKeeper1 = freeTCPPorts.get(2);
    final int bsPort1 = bsKeeper1.getPort();
    final Keeper bsKeeper2 = freeTCPPorts.get(3);
    final int bsPort2 = bsKeeper2.getPort();
    final Keeper bsKeeper3 = freeTCPPorts.get(4);
    final int bsPort3 = bsKeeper3.getPort();
    final Keeper bsKeeper4 = freeTCPPorts.get(5);
    final int bsPort4 = bsKeeper4.getPort();

    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]" + "," + host0 + "[" + port2 + "]";

    final Properties dsProps = new Properties();
    dsProps.setProperty(LOCATORS, locators);
    dsProps.setProperty(MCAST_PORT, "0");
    dsProps.setProperty(LOG_LEVEL, String.valueOf(logger.getLevel()));
    dsProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    keeper1.release();
    vm0.invoke(() -> startLocatorOnPort(port1, dsProps, null));

    keeper2.release();
    vm3.invoke(() -> startLocatorOnPort(port2, dsProps, "locator2HNFC"));

    vm1.invoke(() -> createCache(locators, null));
    vm2.invoke(() -> createCache(locators, null));

    bsKeeper1.release();
    vm1.invoke(() -> startBridgeServerOnPort(bsPort1, "bs1Group1", "bs1Group2"));
    bsKeeper3.release();
    vm1.invoke(() -> startBridgeServerOnPort(bsPort3, "bs3Group1", "bs3Group2"));
    bsKeeper2.release();
    vm2.invoke(() -> startBridgeServerOnPort(bsPort2, "bs2Group1", "bs2Group2"));
    bsKeeper4.release();
    vm2.invoke(() -> startBridgeServerOnPort(bsPort4, "bs4Group1", "bs4Group2"));

    // verify that locators know about each other
    vm0.invoke(() -> {
      assertTrue(Locator.hasLocator());
      InternalLocator l = (InternalLocator) Locator.getLocator();
      DistributionAdvisee advisee = l.getServerLocatorAdvisee();
      ControllerAdvisor ca = (ControllerAdvisor) advisee.getDistributionAdvisor();
      List others = ca.fetchControllers();
      assertThat(others.size()).isEqualTo(1);
      {
        ControllerAdvisor.ControllerProfile cp =
            (ControllerAdvisor.ControllerProfile) others.get(0);
        assertThat(cp.getPort()).isEqualTo(port2);
        assertThat(cp.getHost()).isEqualTo("locator2HNFC");
      }

      others = ca.fetchBridgeServers();
      assertThat(others.size()).isEqualTo(4);
      for (Object other : others) {
        CacheServerAdvisor.CacheServerProfile bsp =
            (CacheServerAdvisor.CacheServerProfile) other;
        if (bsp.getPort() == bsPort1) {
          assertThat(new String[] {"bs1Group1", "bs1Group2"}).isEqualTo(bsp.getGroups());
        } else {
          comparePortGroups(bsp, bsPort2, bsPort3, bsPort4);
        }
      }
    });

    vm3.invoke(
        () -> verifyLocatorOnOtherPort(port1, bsPort1, bsPort2, bsPort3, bsPort4, "bs3Group1",
            "bs3Group2", "bs4Group1",
            "bs4Group2"));
    vm1.invoke(
        () -> verifyBridgeServerViewOnTwoPorts(port1, port2));
    vm2.invoke(
        () -> verifyBridgeServerViewOnTwoPorts(port1, port2));

    vm1.invoke(this::stopBridgeServer);

    // now check to see if everyone else noticed it going away
    vm0.invoke(() -> verifyOtherLocatorOnPortWithName(port2, bsPort2, bsPort3, bsPort4, "bs3Group1",
        "bs3Group2",
        "bs4Group1", "bs4Group2"));

    vm3.invoke(() -> {
      assertTrue(Locator.hasLocator());
      InternalLocator l = (InternalLocator) Locator.getLocator();
      DistributionAdvisee advisee = l.getServerLocatorAdvisee();
      ControllerAdvisor ca = (ControllerAdvisor) advisee.getDistributionAdvisor();
      List others = ca.fetchControllers();
      assertThat(others.size()).isEqualTo(1);
      {
        ControllerAdvisor.ControllerProfile cp =
            (ControllerAdvisor.ControllerProfile) others.get(0);
        assertThat(cp.getPort()).isEqualTo(port1);
      }
      others = ca.fetchBridgeServers();
      assertThat(others.size()).isEqualTo(3);
      for (Object other : others) {
        CacheServerAdvisor.CacheServerProfile bsp =
            (CacheServerAdvisor.CacheServerProfile) other;
        comparePortGroups(bsp, bsPort2, bsPort3, bsPort4);
      }
    });

    vm0.invoke(this::stopLocatorAndCheckIt);

    // now make sure everyone else saw the locator go away
    vm3.invoke(this::verifyLocatorStopped);
    vm2.invoke(() -> verifyBridgeServerSawLocatorStop(port2));
    vm1.invoke(() -> verifyBridgeServerSawLocatorStopWithName(port2));

    // restart bridge server 1 and see if controller sees it
    vm1.invoke(this::restartBridgeServer);

    vm3.invoke(() -> verifyBridgeServerRestart(bsPort1, bsPort2, bsPort3, bsPort4, "bs3Group1",
        "bs3Group2",
        "bs4Group1",
        "bs4Group2"));

    vm1.invoke("Disconnect from " + locators, this::safeCloseCache);
    vm2.invoke("Disconnect from " + locators, this::safeCloseCache);
    // now make sure controller saw all bridge servers stop

    vm3.invoke(this::verifyLocatorsAndBridgeServersStoppped);
    vm3.invoke(this::stopLocatorAndCheckIt);
  }


  @Test
  public void test2by2usingGroups() {
    disconnectAllFromDS();

    VM vm0 = VM.getVM(0);
    VM vm1 = VM.getVM(1);
    VM vm2 = VM.getVM(2);
    VM vm3 = VM.getVM(3);

    List<Keeper> freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPortKeepers(6);
    final Keeper keeper1 = freeTCPPorts.get(0);
    final int port1 = keeper1.getPort();
    final Keeper keeper2 = freeTCPPorts.get(1);
    final int port2 = keeper2.getPort();
    final Keeper bsKeeper1 = freeTCPPorts.get(2);
    final int bsPort1 = bsKeeper1.getPort();
    final Keeper bsKeeper2 = freeTCPPorts.get(3);
    final int bsPort2 = bsKeeper2.getPort();
    final Keeper bsKeeper3 = freeTCPPorts.get(4);
    final int bsPort3 = bsKeeper3.getPort();
    final Keeper bsKeeper4 = freeTCPPorts.get(5);
    final int bsPort4 = bsKeeper4.getPort();

    final String host0 = NetworkUtils.getServerHostName();
    final String locators = host0 + "[" + port1 + "]" + "," + host0 + "[" + port2 + "]";

    final Properties dsProps = new Properties();
    dsProps.setProperty(LOCATORS, locators);
    dsProps.setProperty(MCAST_PORT, "0");
    dsProps.setProperty(LOG_LEVEL, String.valueOf(logger.getLevel()));
    dsProps.setProperty(ENABLE_CLUSTER_CONFIGURATION, "false");

    keeper1.release();
    vm0.invoke(() -> startLocatorOnPort(port1, dsProps, null));

    keeper2.release();
    vm3.invoke(() -> startLocatorOnPort(port2, dsProps, "locator2HNFC"));

    vm1.invoke(() -> createCache(locators, "bs1Group1, bs1Group2"));
    vm2.invoke(() -> createCache(locators, "bs2Group1, bs2Group2"));

    bsKeeper1.release();
    vm1.invoke(() -> startBridgeServerOnPort(bsPort1));
    bsKeeper3.release();
    vm1.invoke(() -> startBridgeServerOnPort(bsPort3));
    bsKeeper2.release();
    vm2.invoke(() -> startBridgeServerOnPort(bsPort2));
    bsKeeper4.release();
    vm2.invoke(() -> startBridgeServerOnPort(bsPort4));

    // verify that locators know about each other
    vm0.invoke(
        () -> verifyLocatorOnOtherPort(port2, bsPort1, bsPort2, bsPort3, bsPort4, "bs1Group1",
            "bs1Group2", "bs2Group1", "bs2Group2"));
    vm3.invoke(
        () -> verifyLocatorOnOtherPort(port1, bsPort1, bsPort2, bsPort3, bsPort4, "bs1Group1",
            "bs1Group2", "bs2Group1", "bs2Group2"));

    vm1.invoke(
        () -> verifyBridgeServerViewOnTwoPorts(port1, port2));
    vm2.invoke(
        () -> verifyBridgeServerViewOnTwoPorts(port1, port2));

    vm1.invoke(this::stopBridgeServer);

    // now check to see if everyone else noticed it going away
    vm0.invoke(() -> verifyOtherLocatorOnPortWithName(port2, bsPort2, bsPort3, bsPort4, "bs1Group1",
        "bs1Group2",
        "bs2Group1",
        "bs2Group2"));

    vm3.invoke(() -> {
      assertTrue(Locator.hasLocator());
      InternalLocator l = (InternalLocator) Locator.getLocator();
      DistributionAdvisee advisee = l.getServerLocatorAdvisee();
      ControllerAdvisor ca = (ControllerAdvisor) advisee.getDistributionAdvisor();
      List others = ca.fetchControllers();
      assertThat(others.size()).isEqualTo(1);
      {
        ControllerAdvisor.ControllerProfile cp =
            (ControllerAdvisor.ControllerProfile) others.get(0);
        assertThat(cp.getPort()).isEqualTo(port1);
      }
      others = ca.fetchBridgeServers();
      assertThat(others.size()).isEqualTo(3);
      for (Object other : others) {
        CacheServerAdvisor.CacheServerProfile bsp =
            (CacheServerAdvisor.CacheServerProfile) other;
        if (bsp.getPort() == bsPort2) {
          assertThat(new String[] {"bs2Group1", "bs2Group2"}).isEqualTo(bsp.getGroups());
        } else if (bsp.getPort() == bsPort3) {
          assertThat(new String[] {"bs1Group1", "bs1Group2"}).isEqualTo(bsp.getGroups());
        } else if (bsp.getPort() == bsPort4) {
          assertThat(new String[] {"bs2Group1", "bs2Group2"}).isEqualTo(bsp.getGroups());
        } else {
          fail("unexpected port " + bsp.getPort() + " in " + bsp);
        }
      }
    });

    vm0.invoke(this::stopLocatorAndCheckIt);

    // now make sure everyone else saw the locator go away
    vm3.invoke(() -> await().untilAsserted(this::verifyLocatorStopped));
    vm2.invoke(() -> await().untilAsserted(() -> verifyBridgeServerSawLocatorStop(port2)));
    vm1.invoke(() -> await().untilAsserted(() -> verifyBridgeServerSawLocatorStopWithName(port2)));

    // restart bridge server 1 and see if controller sees it
    vm1.invoke(this::restartBridgeServer);

    vm3.invoke(() -> verifyBridgeServerRestart(bsPort1, bsPort2, bsPort3, bsPort4, "bs1Group1",
        "bs1Group2",
        "bs2Group1", "bs2Group2"));

    vm1.invoke("Disconnect from " + locators, this::safeCloseCache);
    vm2.invoke("Disconnect from " + locators, this::safeCloseCache);
    // now make sure controller saw all bridge servers stop

    vm3.invoke(() -> await().untilAsserted(this::verifyLocatorsAndBridgeServersStoppped));
    vm3.invoke(this::stopLocatorAndCheckIt);
  }

  private void createCache(String locators, String groups) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);
    if (groups != null) {
      props.setProperty(GROUPS, groups);
    }
    props.setProperty(LOG_LEVEL, String.valueOf(logger.getLevel()));
    cache = (InternalCache) new CacheFactory(props).create();
  }


  private void comparePortGroups(CacheServerAdvisor.CacheServerProfile bsp, int bsPort2,
      int bsPort3, int bsPort4) {
    if (bsp.getPort() == bsPort2) {
      assertThat(new String[] {"bs2Group1", "bs2Group2"}).isEqualTo(bsp.getGroups());
    } else if (bsp.getPort() == bsPort3) {
      assertThat(new String[] {"bs3Group1", "bs3Group2"}).isEqualTo(bsp.getGroups());
    } else if (bsp.getPort() == bsPort4) {
      assertThat(new String[] {"bs4Group1", "bs4Group2"}).isEqualTo(bsp.getGroups());
    } else {
      fail("unexpected port " + bsp.getPort() + " in " + bsp);
    }
  }

  private void stopLocatorAndCheckIt() {
    assertTrue(Locator.hasLocator());
    Locator.getLocator().stop();
    assertFalse(Locator.hasLocator());
  }


  private void verifyBridgeServerViewOnTwoPorts(int port1, int port2) {
    Cache c = cache;
    List bslist = c.getCacheServers();
    assertThat(bslist.size()).isEqualTo(2);
    for (Object aBslist : bslist) {
      DistributionAdvisee advisee = (DistributionAdvisee) aBslist;
      CacheServerAdvisor bsa = (CacheServerAdvisor) advisee.getDistributionAdvisor();
      List others = bsa.fetchBridgeServers();
      logger.info("found these bridgeservers in " + advisee + ": " + others);
      assertThat(others.size()).isEqualTo(3);
      others = bsa.fetchControllers();
      assertThat(others.size()).isEqualTo(2);
      for (Object other : others) {
        ControllerAdvisor.ControllerProfile cp =
            (ControllerAdvisor.ControllerProfile) other;
        if (cp.getPort() != port1) {
          if (cp.getPort() == port2) {
            assertThat(cp.getHost()).isEqualTo("locator2HNFC");
            // ok
          } else {
            fail("unexpected port " + cp.getPort() + " in " + cp);
          }
        }
      }
    }
  }

  private void verifyBridgeServerRestart(int bsPort1, int bsPort2, int bsPort3, int bsPort4,
      String bs3Group1, String bs3Group2, String bs4Group1,
      String bs4Group2) {
    assertTrue(Locator.hasLocator());
    InternalLocator l = (InternalLocator) Locator.getLocator();
    DistributionAdvisee advisee = l.getServerLocatorAdvisee();
    ControllerAdvisor ca = (ControllerAdvisor) advisee.getDistributionAdvisor();
    assertThat(ca.fetchControllers().size()).isEqualTo(0);
    List others = ca.fetchBridgeServers();
    assertThat(others.size()).isEqualTo(4);
    for (Object other : others) {
      CacheServerAdvisor.CacheServerProfile bsp =
          (CacheServerAdvisor.CacheServerProfile) other;
      if (bsp.getPort() == bsPort1) {
        assertThat(new String[] {"bs1Group1", "bs1Group2"}).isEqualTo(bsp.getGroups());
        assertThat(bsp.getHost()).isEqualTo("nameForClients");
      } else if (bsp.getPort() == bsPort2) {
        assertThat(new String[] {"bs2Group1", "bs2Group2"}).isEqualTo(bsp.getGroups());
        assertThat(bsp.getHost()).isNotEqualTo("nameForClients");
      } else {
        comparePortGroupsTwo(bsPort3, bsPort4, bs3Group1, bs3Group2, bs4Group1, bs4Group2, bsp);
      }
    }
  }

  private void comparePortGroupsTwo(int bsPort3, int bsPort4, String bs3Group1, String bs3Group2,
      String bs4Group1, String bs4Group2,
      CacheServerAdvisor.CacheServerProfile bsp) {
    if (bsp.getPort() == bsPort3) {
      assertThat(Arrays.asList(bsp.getGroups())).isEqualTo(Arrays.asList(bs3Group1, bs3Group2));
    } else if (bsp.getPort() == bsPort4) {
      assertThat(Arrays.asList(bsp.getGroups())).isEqualTo(Arrays.asList(bs4Group1, bs4Group2));
    } else {
      fail("unexpected port " + bsp.getPort() + " in " + bsp);
    }
  }

  private void verifyLocatorStopped() {
    assertTrue(Locator.hasLocator());
    InternalLocator l = (InternalLocator) Locator.getLocator();
    DistributionAdvisee advisee = l.getServerLocatorAdvisee();
    ControllerAdvisor ca = (ControllerAdvisor) advisee.getDistributionAdvisor();
    List others = ca.fetchControllers();
    assertThat(others.size()).isEqualTo(0);
  }

  private void verifyBridgeServerSawLocatorStop(int port2) {
    Cache c = cache;
    List bslist = c.getCacheServers();
    assertThat(bslist.size()).isEqualTo(2);
    for (Object aBslist : bslist) {
      DistributionAdvisee advisee = (DistributionAdvisee) aBslist;
      CacheServerAdvisor bsa = (CacheServerAdvisor) advisee.getDistributionAdvisor();
      List others = bsa.fetchControllers();
      assertThat(others.size()).isEqualTo(1);
      verifyHostNameForPort(port2, others);
    }
  }

  private void verifyHostNameForPort(int port2, List others) {
    for (Object other : others) {
      ControllerAdvisor.ControllerProfile cp =
          (ControllerAdvisor.ControllerProfile) other;
      if (cp.getPort() == port2) {
        assertThat(cp.getHost()).isEqualTo("locator2HNFC");
        // ok
      } else {
        fail("unexpected port " + cp.getPort() + " in " + cp);
      }
    }
  }

  private void verifyLocatorOnOtherPort(int port1, int bsPort1, int bsPort2, int bsPort3,
      int bsPort4, String bs3Group1, String bs3Group2,
      String bs4Group1, String bs4Group2) {
    assertTrue(Locator.hasLocator());
    InternalLocator l = (InternalLocator) Locator.getLocator();
    DistributionAdvisee advisee = l.getServerLocatorAdvisee();
    ControllerAdvisor ca = (ControllerAdvisor) advisee.getDistributionAdvisor();
    List others = ca.fetchControllers();
    assertThat(others.size()).isEqualTo(1);
    {
      ControllerAdvisor.ControllerProfile cp =
          (ControllerAdvisor.ControllerProfile) others.get(0);
      assertThat(cp.getPort()).isEqualTo(port1);
    }
    others = ca.fetchBridgeServers();
    assertThat(others.size()).isEqualTo(4);
    for (Object other : others) {
      CacheServerAdvisor.CacheServerProfile bsp =
          (CacheServerAdvisor.CacheServerProfile) other;
      if (bsp.getPort() == bsPort1) {
        assertThat(new String[] {"bs1Group1", "bs1Group2"}).isEqualTo(bsp.getGroups());
      } else if (bsp.getPort() == bsPort2) {
        assertThat(new String[] {"bs2Group1", "bs2Group2"}).isEqualTo(bsp.getGroups());
      } else {
        comparePortGroupsTwo(bsPort3, bsPort4, bs3Group1, bs3Group2, bs4Group1, bs4Group2, bsp);
      }
    }
  }

  private void startBridgeServerOnPort(int bsPort1, String bs1Group1, String bs1Group2) {
    try {
      Cache c = cache;
      CacheServer bs = c.addCacheServer();
      bs.setPort(bsPort1);
      bs.setGroups(new String[] {bs1Group1, bs1Group2});
      bs.start();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void startLocatorOnPort(int port1, Properties dsProps, String name) {
    File logFile = new File(getUniqueName() + "-locator" + port1 + ".log");
    try {
      Locator.startLocatorAndDS(port1, logFile, null, dsProps, true, true, name);
    } catch (IllegalStateException | IOException ex) {
      Assertions.fail("While starting locator on port " + port1, ex);
    }
  }


  private void safeCloseCache() {
    if (cache != null) {
      cache.close();
    }
  }

  private void startBridgeServerOnPort(int bsPort4) {
    try {
      Cache c = cache;
      CacheServer bs = c.addCacheServer();
      bs.setPort(bsPort4);
      bs.start();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void stopBridgeServer() {
    Cache c = cache;
    List bslist = c.getCacheServers();
    assertThat(bslist.size()).isEqualTo(2);
    CacheServer bs = (CacheServer) bslist.get(0);
    bs.stop();
  }

  private void verifyLocatorsAndBridgeServersStoppped() {
    assertTrue(Locator.hasLocator());
    InternalLocator l = (InternalLocator) Locator.getLocator();
    DistributionAdvisee advisee = l.getServerLocatorAdvisee();
    ControllerAdvisor ca = (ControllerAdvisor) advisee.getDistributionAdvisor();
    assertThat(ca.fetchControllers().size()).isEqualTo(0);
    assertThat(ca.fetchBridgeServers().size()).isEqualTo(0);
  }

  private void restartBridgeServer() {
    try {
      Cache c = cache;
      List bslist = c.getCacheServers();
      assertThat(bslist.size()).isEqualTo(2);
      CacheServer bs = (CacheServer) bslist.get(0);
      bs.setHostnameForClients("nameForClients");
      bs.start();
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  private void verifyBridgeServerSawLocatorStopWithName(int port2) {
    Cache c = cache;
    List bslist = c.getCacheServers();
    assertThat(bslist.size()).isEqualTo(2);
    for (int i = 0; i < bslist.size(); i++) {
      DistributionAdvisee advisee = (DistributionAdvisee) bslist.get(i);
      if (i == 0) {
        // skip this one since it is stopped
        continue;
      }
      CacheServerAdvisor bsa = (CacheServerAdvisor) advisee.getDistributionAdvisor();
      List others = bsa.fetchControllers();
      assertThat(others.size()).isEqualTo(1);
      verifyHostNameForPort(port2, others);
    }
  }

  private void verifyOtherLocatorOnPortWithName(int port2, int bsPort2, int bsPort3, int bsPort4,
      String bs1Group1, String bs1Group2,
      String bs2Group1, String bs2Group2) {
    assertTrue(Locator.hasLocator());
    InternalLocator l = (InternalLocator) Locator.getLocator();
    DistributionAdvisee advisee = l.getServerLocatorAdvisee();
    ControllerAdvisor ca = (ControllerAdvisor) advisee.getDistributionAdvisor();
    List others = ca.fetchControllers();
    assertThat(others.size()).isEqualTo(1);
    {
      ControllerAdvisor.ControllerProfile cp =
          (ControllerAdvisor.ControllerProfile) others.get(0);
      assertThat(cp.getPort()).isEqualTo(port2);
      assertThat(cp.getHost()).isEqualTo("locator2HNFC");
    }

    others = ca.fetchBridgeServers();
    assertThat(others.size()).isEqualTo(3);
    for (Object other : others) {
      CacheServerAdvisor.CacheServerProfile bsp =
          (CacheServerAdvisor.CacheServerProfile) other;
      if (bsp.getPort() == bsPort2) {
        assertThat(new String[] {"bs2Group1", "bs2Group2"}).isEqualTo(bsp.getGroups());
      } else if (bsp.getPort() == bsPort3) {
        assertThat(new String[] {bs1Group1, bs1Group2}).isEqualTo(bsp.getGroups());
      } else if (bsp.getPort() == bsPort4) {
        assertThat(new String[] {bs2Group1, bs2Group2}).isEqualTo(bsp.getGroups());
      } else {
        fail("unexpected port " + bsp.getPort() + " in " + bsp);
      }
    }
  }
}
