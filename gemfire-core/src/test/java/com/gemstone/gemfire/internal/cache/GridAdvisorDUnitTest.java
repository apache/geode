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
package com.gemstone.gemfire.internal.cache;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.server.CacheServer;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.Locator;
import com.gemstone.gemfire.distributed.internal.DistributionAdvisee;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.distributed.internal.InternalDistributedSystem;
import com.gemstone.gemfire.distributed.internal.InternalLocator;
import com.gemstone.gemfire.internal.AvailablePort.Keeper;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.test.dunit.Assert;
import com.gemstone.gemfire.test.dunit.DistributedTestCase;
import com.gemstone.gemfire.test.dunit.Host;
import com.gemstone.gemfire.test.dunit.LogWriterUtils;
import com.gemstone.gemfire.test.dunit.NetworkUtils;
import com.gemstone.gemfire.test.dunit.SerializableRunnable;
import com.gemstone.gemfire.test.dunit.VM;

/**
 * Tests the GridAdvisor
 *
 * @author darrel
 * @since 5.7
 */
public class GridAdvisorDUnitTest extends DistributedTestCase {

  public GridAdvisorDUnitTest(String name) {
    super(name);
  }

  ////////  Test Methods

  /**
   * Tests 2 controllers and 2 bridge servers
   */
  public void test2by2() throws Exception {
    disconnectAllFromDS();

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

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

    final String host0 = NetworkUtils.getServerHostName(host); 
    final String locators =   host0 + "[" + port1 + "]" + "," 
                            + host0 + "[" + port2 + "]";

    final Properties dsProps = new Properties();
    dsProps.setProperty("locators", locators);
    dsProps.setProperty("mcast-port", "0");
    dsProps.setProperty("log-level", LogWriterUtils.getDUnitLogLevel());
    dsProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    keeper1.release();
    vm0.invoke(new SerializableRunnable("Start locator on " + port1) {
        public void run() {
          File logFile = new File(getUniqueName() + "-locator" + port1
                                  + ".log");
          try {
            Locator.startLocatorAndDS(port1, logFile, null, dsProps, true, true, null);
          } catch (IOException ex) {
            Assert.fail("While starting locator on port " + port1, ex);
          }
        }
      });
      
    //try { Thread.currentThread().sleep(4000); } catch (InterruptedException ie) { }
    
    keeper2.release();
    vm3.invoke(new SerializableRunnable("Start locators on " + port2) {
        public void run() {
          File logFile = new File(getUniqueName() + "-locator" +
                                  port2 + ".log");
          try {
            Locator.startLocatorAndDS(port2, logFile, null, dsProps, true, true, "locator2HNFC");

          } catch (IOException ex) {
            Assert.fail("While starting locator on port " + port2, ex);
          }
        }
      });

    SerializableRunnable connect =
      new SerializableRunnable("Connect to " + locators) {
          public void run() {
            Properties props = new Properties();
            props.setProperty("mcast-port", "0");
            props.setProperty("locators", locators);
            dsProps.setProperty("log-level", LogWriterUtils.getDUnitLogLevel());
            CacheFactory.create(DistributedSystem.connect(props));
          }
        };
    vm1.invoke(connect);
    vm2.invoke(connect);
    SerializableRunnable startBS1 =
      new SerializableRunnable("start bridgeServer on " + bsPort1) {
        public void run() {
          try {
            Cache c = CacheFactory.getAnyInstance();
            CacheServer bs = c.addCacheServer();
            bs.setPort(bsPort1);
            bs.setGroups(new String[] {"bs1Group1", "bs1Group2"});
            bs.start();
          } catch (IOException ex) {
            RuntimeException re = new RuntimeException();
            re.initCause(ex);
            throw re;
          }
        }
      };
      SerializableRunnable startBS3 =
        new SerializableRunnable("start bridgeServer on " + bsPort3) {
          public void run() {
            try {
              Cache c = CacheFactory.getAnyInstance();
              CacheServer bs = c.addCacheServer();
              bs.setPort(bsPort3);
              bs.setGroups(new String[] {"bs3Group1", "bs3Group2"});
              bs.start();
            } catch (IOException ex) {
              RuntimeException re = new RuntimeException();
              re.initCause(ex);
              throw re;
            }
          }
        };

    bsKeeper1.release();
    vm1.invoke(startBS1);
    bsKeeper3.release();
    vm1.invoke(startBS3);
    bsKeeper2.release();
    vm2.invoke(new SerializableRunnable("start bridgeServer on " + bsPort2) {
      public void run() {
        try {
          Cache c = CacheFactory.getAnyInstance();
          CacheServer bs = c.addCacheServer();
          bs.setPort(bsPort2);
          bs.setGroups(new String[] {"bs2Group1", "bs2Group2"});
          bs.start();
        } catch (IOException ex) {
          RuntimeException re = new RuntimeException();
          re.initCause(ex);
          throw re;
        }
      }
    });
    bsKeeper4.release();
    vm2.invoke(new SerializableRunnable("start bridgeServer on " + bsPort4) {
      public void run() {
        try {
          Cache c = CacheFactory.getAnyInstance();
          CacheServer bs = c.addCacheServer();
          bs.setPort(bsPort4);
          bs.setGroups(new String[] {"bs4Group1", "bs4Group2"});
          bs.start();
        } catch (IOException ex) {
          RuntimeException re = new RuntimeException();
          re.initCause(ex);
          throw re;
        }
      }
    });

    // verify that locators know about each other
    vm0.invoke(new SerializableRunnable("Verify other locator on " + port2) {
        public void run() {
          assertTrue(Locator.hasLocator());
            InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            List others = ca.fetchControllers();
            assertEquals(1, others.size());
            {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(0);
              assertEquals(port2, cp.getPort());
              assertEquals("locator2HNFC", cp.getHost());
            }

            others = ca.fetchBridgeServers();
            assertEquals(4, others.size());
            for (int j=0; j < others.size(); j++) {
              CacheServerAdvisor.CacheServerProfile bsp =
                (CacheServerAdvisor.CacheServerProfile)others.get(j);
              if (bsp.getPort() == bsPort1) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort2) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort3) {
                assertEquals(Arrays.asList(new String[] {"bs3Group1", "bs3Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort4) {
                assertEquals(Arrays.asList(new String[] {"bs4Group1", "bs4Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else {
                fail("unexpected port " + bsp.getPort() + " in " + bsp);
              }
            }
        }
      });
    vm3.invoke(new SerializableRunnable("Verify other locator on " + port1) {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            List others = ca.fetchControllers();
            assertEquals(1, others.size());
            {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(0);
              assertEquals(port1, cp.getPort());
            }
            others = ca.fetchBridgeServers();
            assertEquals(4, others.size());
            for (int j=0; j < others.size(); j++) {
              CacheServerAdvisor.CacheServerProfile bsp =
                (CacheServerAdvisor.CacheServerProfile)others.get(j);
              if (bsp.getPort() == bsPort1) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort2) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort3) {
                assertEquals(Arrays.asList(new String[] {"bs3Group1", "bs3Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort4) {
                assertEquals(Arrays.asList(new String[] {"bs4Group1", "bs4Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else {
                fail("unexpected port " + bsp.getPort() + " in " + bsp);
              }
            }
        }
      });
    vm1.invoke(new SerializableRunnable("Verify bridge server view on " + bsPort1 + " and on " + bsPort3) {
        public void run() {
          Cache c = CacheFactory.getAnyInstance();
          List bslist = c.getCacheServers();
          assertEquals(2, bslist.size());
          for (int i=0; i < bslist.size(); i++) {
            DistributionAdvisee advisee = (DistributionAdvisee)bslist.get(i);
            CacheServerAdvisor bsa = (CacheServerAdvisor)advisee.getDistributionAdvisor();
            List others = bsa.fetchBridgeServers();
            LogWriterUtils.getLogWriter().info("found these bridgeservers in " + advisee + ": " + others);
            assertEquals(3, others.size());
            others = bsa.fetchControllers();
            assertEquals(2, others.size());
            for (int j=0; j < others.size(); j++) {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(j);
              if (cp.getPort() == port1) {
                // ok
              } else if (cp.getPort() == port2) {
                assertEquals("locator2HNFC", cp.getHost());
                // ok
              } else {
                fail("unexpected port " + cp.getPort() + " in " + cp);
              }
            }
          }
        }
      });
    vm2.invoke(new SerializableRunnable("Verify bridge server view on " + bsPort2 + " and on " + bsPort4) {
        public void run() {
          Cache c = CacheFactory.getAnyInstance();
          List bslist = c.getCacheServers();
          assertEquals(2, bslist.size());
          for (int i=0; i < bslist.size(); i++) {
            DistributionAdvisee advisee = (DistributionAdvisee)bslist.get(i);
            CacheServerAdvisor bsa = (CacheServerAdvisor)advisee.getDistributionAdvisor();
            List others = bsa.fetchBridgeServers();
            LogWriterUtils.getLogWriter().info("found these bridgeservers in " + advisee + ": " + others);
            assertEquals(3, others.size());
            others = bsa.fetchControllers();
            assertEquals(2, others.size());
            for (int j=0; j < others.size(); j++) {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(j);
              if (cp.getPort() == port1) {
                // ok
              } else if (cp.getPort() == port2) {
                assertEquals("locator2HNFC", cp.getHost());
                // ok
              } else {
                fail("unexpected port " + cp.getPort() + " in " + cp);
              }
            }
          }
        }
      });

    SerializableRunnable stopBS =
      new SerializableRunnable("stop bridge server") {
          public void run() {
            Cache c = CacheFactory.getAnyInstance();
            List bslist = c.getCacheServers();
            assertEquals(2, bslist.size());
            CacheServer bs = (CacheServer)bslist.get(0);
            bs.stop();
          }
        };
    vm1.invoke(stopBS);
    
    // now check to see if everyone else noticed him going away
    vm0.invoke(new SerializableRunnable("Verify other locator on " + port2) {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            List others = ca.fetchControllers();
            assertEquals(1, others.size());
            {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(0);
              assertEquals(port2, cp.getPort());
              assertEquals("locator2HNFC", cp.getHost());
            }

            others = ca.fetchBridgeServers();
            assertEquals(3, others.size());
            for (int j=0; j < others.size(); j++) {
              CacheServerAdvisor.CacheServerProfile bsp =
                (CacheServerAdvisor.CacheServerProfile)others.get(j);
              if (bsp.getPort() == bsPort2) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort3) {
                assertEquals(Arrays.asList(new String[] {"bs3Group1", "bs3Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort4) {
                assertEquals(Arrays.asList(new String[] {"bs4Group1", "bs4Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else {
                fail("unexpected port " + bsp.getPort() + " in " + bsp);
              }
            }
        }
      });
    vm3.invoke(new SerializableRunnable("Verify other locator on " + port1) {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            List others = ca.fetchControllers();
            assertEquals(1, others.size());
            {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(0);
              assertEquals(port1, cp.getPort());
            }
            others = ca.fetchBridgeServers();
            assertEquals(3, others.size());
            for (int j=0; j < others.size(); j++) {
              CacheServerAdvisor.CacheServerProfile bsp =
                (CacheServerAdvisor.CacheServerProfile)others.get(j);
              if (bsp.getPort() == bsPort2) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort3) {
                assertEquals(Arrays.asList(new String[] {"bs3Group1", "bs3Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort4) {
                assertEquals(Arrays.asList(new String[] {"bs4Group1", "bs4Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else {
                fail("unexpected port " + bsp.getPort() + " in " + bsp);
              }
            }
        }
      });

    SerializableRunnable disconnect =
      new SerializableRunnable("Disconnect from " + locators) {
          public void run() {
            Properties props = new Properties();
            props.setProperty("mcast-port", "0");
            props.setProperty("locators", locators);
            DistributedSystem.connect(props).disconnect();
          }
        };
    SerializableRunnable stopLocator = 
      new SerializableRunnable("Stop locator") {
          public void run() {
            assertTrue(Locator.hasLocator());
            Locator.getLocator().stop();
            assertFalse(Locator.hasLocator());
          }
        };

    vm0.invoke(stopLocator);

    // now make sure everyone else saw the locator go away
    vm3.invoke(new SerializableRunnable("Verify locator stopped ") {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            List others = ca.fetchControllers();
            assertEquals(0, others.size());
          }
      });
    vm2.invoke(new SerializableRunnable("Verify bridge server saw locator stop") {
        public void run() {
          Cache c = CacheFactory.getAnyInstance();
          List bslist = c.getCacheServers();
          assertEquals(2, bslist.size());
          for (int i=0; i < bslist.size(); i++) {
            DistributionAdvisee advisee = (DistributionAdvisee)bslist.get(i);
            CacheServerAdvisor bsa = (CacheServerAdvisor)advisee.getDistributionAdvisor();
            List others = bsa.fetchControllers();
            assertEquals(1, others.size());
            for (int j=0; j < others.size(); j++) {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(j);
              if (cp.getPort() == port2) {
                assertEquals("locator2HNFC", cp.getHost());
                // ok
              } else {
                fail("unexpected port " + cp.getPort() + " in " + cp);
              }
            }
          }
        }
      });
    vm1.invoke(new SerializableRunnable("Verify bridge server saw locator stop") {
        public void run() {
          Cache c = CacheFactory.getAnyInstance();
          List bslist = c.getCacheServers();
          assertEquals(2, bslist.size());
          for (int i=0; i < bslist.size(); i++) {
            DistributionAdvisee advisee = (DistributionAdvisee)bslist.get(i);
            if (i == 0) {
              // skip this one since it is stopped
              continue;
            }
            CacheServerAdvisor bsa = (CacheServerAdvisor)advisee.getDistributionAdvisor();
            List others = bsa.fetchControllers();
            assertEquals(1, others.size());
            for (int j=0; j < others.size(); j++) {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(j);
              if (cp.getPort() == port2) {
                assertEquals("locator2HNFC", cp.getHost());
                // ok
              } else {
                fail("unexpected port " + cp.getPort() + " in " + cp);
              }
            }
          }
        }
      });

    SerializableRunnable restartBS =
      new SerializableRunnable("restart bridge server") {
          public void run() {
            try {
              Cache c = CacheFactory.getAnyInstance();
              List bslist = c.getCacheServers();
              assertEquals(2, bslist.size());
              CacheServer bs = (CacheServer)bslist.get(0);
              bs.setHostnameForClients("nameForClients");
              bs.start();
            } catch (IOException ex) {
              RuntimeException re = new RuntimeException();
              re.initCause(ex);
              throw re;
            }
          }
        };
    // restart bridge server 1 and see if controller sees it
    vm1.invoke(restartBS);
    
    vm3.invoke(new SerializableRunnable("Verify bridge server restart ") {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            assertEquals(0, ca.fetchControllers().size());
            List others = ca.fetchBridgeServers();
            assertEquals(4, others.size());
            for (int j=0; j < others.size(); j++) {
              CacheServerAdvisor.CacheServerProfile bsp =
                (CacheServerAdvisor.CacheServerProfile)others.get(j);
              if (bsp.getPort() == bsPort1) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
                assertEquals("nameForClients", bsp.getHost());
              } else if (bsp.getPort() == bsPort2) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
                assertFalse(bsp.getHost().equals("nameForClients"));
              } else if (bsp.getPort() == bsPort3) {
                assertEquals(Arrays.asList(new String[] {"bs3Group1", "bs3Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort4) {
                assertEquals(Arrays.asList(new String[] {"bs4Group1", "bs4Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else {
                fail("unexpected port " + bsp.getPort() + " in " + bsp);
              }
            }
        }
      });

    vm1.invoke(disconnect);
    vm2.invoke(disconnect);
    // now make sure controller saw all bridge servers stop

    vm3.invoke(new SerializableRunnable("Verify locator stopped ") {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            assertEquals(0, ca.fetchControllers().size());
            assertEquals(0, ca.fetchBridgeServers().size());
          }
      });
    vm3.invoke(stopLocator);
  }
  public void test2by2usingGroups() throws Exception {
    disconnectAllFromDS();

    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);
    VM vm3 = host.getVM(3);

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

    final String host0 = NetworkUtils.getServerHostName(host); 
    final String locators =   host0 + "[" + port1 + "]" + "," 
                            + host0 + "[" + port2 + "]";

    final Properties dsProps = new Properties();
    dsProps.setProperty("locators", locators);
    dsProps.setProperty("mcast-port", "0");
    dsProps.setProperty("log-level", LogWriterUtils.getDUnitLogLevel());
    dsProps.setProperty(DistributionConfig.ENABLE_CLUSTER_CONFIGURATION_NAME, "false");
    
    keeper1.release();
    vm0.invoke(new SerializableRunnable("Start locators on " + port1) {
        public void run() {
          File logFile = new File(getUniqueName() + "-locator" + port1
                                  + ".log");
          try {
            Locator.startLocatorAndDS(port1, logFile, null, dsProps, true, true, null);
          } catch (IOException ex) {
            Assert.fail("While starting locator on port " + port1, ex);
          }
        }
      });
      
    //try { Thread.currentThread().sleep(4000); } catch (InterruptedException ie) { }
    
    keeper2.release();
    vm3.invoke(new SerializableRunnable("Start locators on " + port2) {
        public void run() {
          File logFile = new File(getUniqueName() + "-locator" +
                                  port2 + ".log");
          try {
            Locator.startLocatorAndDS(port2, logFile, null, dsProps, true, true, "locator2HNFC");

          } catch (IOException ex) {
            Assert.fail("While starting locator on port " + port2, ex);
          }
        }
      });

    vm1.invoke(new SerializableRunnable("Connect to " + locators) {
      public void run() {
        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", locators);
        props.setProperty("groups", "bs1Group1, bs1Group2");
        props.setProperty("log-level", LogWriterUtils.getDUnitLogLevel());
        CacheFactory.create(DistributedSystem.connect(props));
      }
    });
    vm2.invoke(new SerializableRunnable("Connect to " + locators) {
      public void run() {
        Properties props = new Properties();
        props.setProperty("mcast-port", "0");
        props.setProperty("locators", locators);
        props.setProperty("groups", "bs2Group1, bs2Group2");
        props.setProperty("log-level", LogWriterUtils.getDUnitLogLevel());
        CacheFactory.create(DistributedSystem.connect(props));
      }
    });

    SerializableRunnable startBS1 =
      new SerializableRunnable("start bridgeServer on " + bsPort1) {
        public void run() {
          try {
            Cache c = CacheFactory.getAnyInstance();
            CacheServer bs = c.addCacheServer();
            bs.setPort(bsPort1);
            bs.start();
          } catch (IOException ex) {
            RuntimeException re = new RuntimeException();
            re.initCause(ex);
            throw re;
          }
        }
      };
      SerializableRunnable startBS3 =
        new SerializableRunnable("start bridgeServer on " + bsPort3) {
          public void run() {
            try {
              Cache c = CacheFactory.getAnyInstance();
              CacheServer bs = c.addCacheServer();
              bs.setPort(bsPort3);
              bs.start();
            } catch (IOException ex) {
              RuntimeException re = new RuntimeException();
              re.initCause(ex);
              throw re;
            }
          }
        };

    bsKeeper1.release();
    vm1.invoke(startBS1);
    bsKeeper3.release();
    vm1.invoke(startBS3);
    bsKeeper2.release();
    vm2.invoke(new SerializableRunnable("start bridgeServer on " + bsPort2) {
      public void run() {
        try {
          Cache c = CacheFactory.getAnyInstance();
          CacheServer bs = c.addCacheServer();
          bs.setPort(bsPort2);
          bs.start();
        } catch (IOException ex) {
          RuntimeException re = new RuntimeException();
          re.initCause(ex);
          throw re;
        }
      }
    });
    bsKeeper4.release();
    vm2.invoke(new SerializableRunnable("start bridgeServer on " + bsPort4) {
      public void run() {
        try {
          Cache c = CacheFactory.getAnyInstance();
          CacheServer bs = c.addCacheServer();
          bs.setPort(bsPort4);
          bs.start();
        } catch (IOException ex) {
          RuntimeException re = new RuntimeException();
          re.initCause(ex);
          throw re;
        }
      }
    });

    // verify that locators know about each other
    vm0.invoke(new SerializableRunnable("Verify other locator on " + port2) {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            List others = ca.fetchControllers();
            assertEquals(1, others.size());
            {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(0);
              assertEquals(port2, cp.getPort());
              assertEquals("locator2HNFC", cp.getHost());
            }

            others = ca.fetchBridgeServers();
            assertEquals(4, others.size());
            for (int j=0; j < others.size(); j++) {
              CacheServerAdvisor.CacheServerProfile bsp =
                (CacheServerAdvisor.CacheServerProfile)others.get(j);
              if (bsp.getPort() == bsPort1) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort2) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort3) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort4) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else {
                fail("unexpected port " + bsp.getPort() + " in " + bsp);
              }
            }
        }
      });
    vm3.invoke(new SerializableRunnable("Verify other locator on " + port1) {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            List others = ca.fetchControllers();
            assertEquals(1, others.size());
            {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(0);
              assertEquals(port1, cp.getPort());
            }
            others = ca.fetchBridgeServers();
            assertEquals(4, others.size());
            for (int j=0; j < others.size(); j++) {
              CacheServerAdvisor.CacheServerProfile bsp =
                (CacheServerAdvisor.CacheServerProfile)others.get(j);
              if (bsp.getPort() == bsPort1) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort2) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort3) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort4) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else {
                fail("unexpected port " + bsp.getPort() + " in " + bsp);
              }
            }
        }
      });
    vm1.invoke(new SerializableRunnable("Verify bridge server view on " + bsPort1 + " and on " + bsPort3) {
        public void run() {
          Cache c = CacheFactory.getAnyInstance();
          List bslist = c.getCacheServers();
          assertEquals(2, bslist.size());
          for (int i=0; i < bslist.size(); i++) {
            DistributionAdvisee advisee = (DistributionAdvisee)bslist.get(i);
            CacheServerAdvisor bsa = (CacheServerAdvisor)advisee.getDistributionAdvisor();
            List others = bsa.fetchBridgeServers();
            LogWriterUtils.getLogWriter().info("found these bridgeservers in " + advisee + ": " + others);
            assertEquals(3, others.size());
            others = bsa.fetchControllers();
            assertEquals(2, others.size());
            for (int j=0; j < others.size(); j++) {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(j);
              if (cp.getPort() == port1) {
                // ok
              } else if (cp.getPort() == port2) {
                assertEquals("locator2HNFC", cp.getHost());
                // ok
              } else {
                fail("unexpected port " + cp.getPort() + " in " + cp);
              }
            }
          }
        }
      });
    vm2.invoke(new SerializableRunnable("Verify bridge server view on " + bsPort2 + " and on " + bsPort4) {
        public void run() {
          Cache c = CacheFactory.getAnyInstance();
          List bslist = c.getCacheServers();
          assertEquals(2, bslist.size());
          for (int i=0; i < bslist.size(); i++) {
            DistributionAdvisee advisee = (DistributionAdvisee)bslist.get(i);
            CacheServerAdvisor bsa = (CacheServerAdvisor)advisee.getDistributionAdvisor();
            List others = bsa.fetchBridgeServers();
            LogWriterUtils.getLogWriter().info("found these bridgeservers in " + advisee + ": " + others);
            assertEquals(3, others.size());
            others = bsa.fetchControllers();
            assertEquals(2, others.size());
            for (int j=0; j < others.size(); j++) {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(j);
              if (cp.getPort() == port1) {
                // ok
              } else if (cp.getPort() == port2) {
                assertEquals("locator2HNFC", cp.getHost());
                // ok
              } else {
                fail("unexpected port " + cp.getPort() + " in " + cp);
              }
            }
          }
        }
      });

    SerializableRunnable stopBS =
      new SerializableRunnable("stop bridge server") {
          public void run() {
            Cache c = CacheFactory.getAnyInstance();
            List bslist = c.getCacheServers();
            assertEquals(2, bslist.size());
            CacheServer bs = (CacheServer)bslist.get(0);
            bs.stop();
          }
        };
    vm1.invoke(stopBS);
    
    // now check to see if everyone else noticed him going away
    vm0.invoke(new SerializableRunnable("Verify other locator on " + port2) {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            List others = ca.fetchControllers();
            assertEquals(1, others.size());
            {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(0);
              assertEquals(port2, cp.getPort());
              assertEquals("locator2HNFC", cp.getHost());
            }

            others = ca.fetchBridgeServers();
            assertEquals(3, others.size());
            for (int j=0; j < others.size(); j++) {
              CacheServerAdvisor.CacheServerProfile bsp =
                (CacheServerAdvisor.CacheServerProfile)others.get(j);
              if (bsp.getPort() == bsPort2) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort3) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort4) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else {
                fail("unexpected port " + bsp.getPort() + " in " + bsp);
              }
            }
        }
      });
    vm3.invoke(new SerializableRunnable("Verify other locator on " + port1) {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            List others = ca.fetchControllers();
            assertEquals(1, others.size());
            {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(0);
              assertEquals(port1, cp.getPort());
            }
            others = ca.fetchBridgeServers();
            assertEquals(3, others.size());
            for (int j=0; j < others.size(); j++) {
              CacheServerAdvisor.CacheServerProfile bsp =
                (CacheServerAdvisor.CacheServerProfile)others.get(j);
              if (bsp.getPort() == bsPort2) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort3) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort4) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else {
                fail("unexpected port " + bsp.getPort() + " in " + bsp);
              }
            }
        }
      });

    SerializableRunnable disconnect =
      new SerializableRunnable("Disconnect from " + locators) {
          public void run() {
            InternalDistributedSystem.getAnyInstance().disconnect();
          }
        };
    SerializableRunnable stopLocator = 
      new SerializableRunnable("Stop locator") {
      public void run() {
        assertTrue(Locator.hasLocator());
        Locator.getLocator().stop();
        assertFalse(Locator.hasLocator());
      }
    };

    vm0.invoke(stopLocator);

    // now make sure everyone else saw the locator go away
    vm3.invoke(new SerializableRunnable("Verify locator stopped ") {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            List others = ca.fetchControllers();
            assertEquals(0, others.size());
          }
      });
    vm2.invoke(new SerializableRunnable("Verify bridge server saw locator stop") {
        public void run() {
          Cache c = CacheFactory.getAnyInstance();
          List bslist = c.getCacheServers();
          assertEquals(2, bslist.size());
          for (int i=0; i < bslist.size(); i++) {
            DistributionAdvisee advisee = (DistributionAdvisee)bslist.get(i);
            CacheServerAdvisor bsa = (CacheServerAdvisor)advisee.getDistributionAdvisor();
            List others = bsa.fetchControllers();
            assertEquals(1, others.size());
            for (int j=0; j < others.size(); j++) {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(j);
              if (cp.getPort() == port2) {
                assertEquals("locator2HNFC", cp.getHost());
                // ok
              } else {
                fail("unexpected port " + cp.getPort() + " in " + cp);
              }
            }
          }
        }
      });
    vm1.invoke(new SerializableRunnable("Verify bridge server saw locator stop") {
        public void run() {
          Cache c = CacheFactory.getAnyInstance();
          List bslist = c.getCacheServers();
          assertEquals(2, bslist.size());
          for (int i=0; i < bslist.size(); i++) {
            DistributionAdvisee advisee = (DistributionAdvisee)bslist.get(i);
            if (i == 0) {
              // skip this one since it is stopped
              continue;
            }
            CacheServerAdvisor bsa = (CacheServerAdvisor)advisee.getDistributionAdvisor();
            List others = bsa.fetchControllers();
            assertEquals(1, others.size());
            for (int j=0; j < others.size(); j++) {
              ControllerAdvisor.ControllerProfile cp =
                (ControllerAdvisor.ControllerProfile)others.get(j);
              if (cp.getPort() == port2) {
                assertEquals("locator2HNFC", cp.getHost());
                // ok
              } else {
                fail("unexpected port " + cp.getPort() + " in " + cp);
              }
            }
          }
        }
      });

    SerializableRunnable restartBS =
      new SerializableRunnable("restart bridge server") {
          public void run() {
            try {
              Cache c = CacheFactory.getAnyInstance();
              List bslist = c.getCacheServers();
              assertEquals(2, bslist.size());
              CacheServer bs = (CacheServer)bslist.get(0);
              bs.setHostnameForClients("nameForClients");
              bs.start();
            } catch (IOException ex) {
              RuntimeException re = new RuntimeException();
              re.initCause(ex);
              throw re;
            }
          }
        };
    // restart bridge server 1 and see if controller sees it
    vm1.invoke(restartBS);
    
    vm3.invoke(new SerializableRunnable("Verify bridge server restart ") {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            assertEquals(0, ca.fetchControllers().size());
            List others = ca.fetchBridgeServers();
            assertEquals(4, others.size());
            for (int j=0; j < others.size(); j++) {
              CacheServerAdvisor.CacheServerProfile bsp =
                (CacheServerAdvisor.CacheServerProfile)others.get(j);
              if (bsp.getPort() == bsPort1) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
                assertEquals("nameForClients", bsp.getHost());
              } else if (bsp.getPort() == bsPort2) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
                assertFalse(bsp.getHost().equals("nameForClients"));
              } else if (bsp.getPort() == bsPort3) {
                assertEquals(Arrays.asList(new String[] {"bs1Group1", "bs1Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else if (bsp.getPort() == bsPort4) {
                assertEquals(Arrays.asList(new String[] {"bs2Group1", "bs2Group2"}),
                             Arrays.asList(bsp.getGroups()));
              } else {
                fail("unexpected port " + bsp.getPort() + " in " + bsp);
              }
            }
        }
      });

    vm1.invoke(disconnect);
    vm2.invoke(disconnect);
    // now make sure controller saw all bridge servers stop

    vm3.invoke(new SerializableRunnable("Verify locator stopped ") {
        public void run() {
          assertTrue(Locator.hasLocator());
          InternalLocator l = (InternalLocator)Locator.getLocator();
            DistributionAdvisee advisee = l.getServerLocatorAdvisee();
            ControllerAdvisor ca = (ControllerAdvisor)advisee.getDistributionAdvisor();
            assertEquals(0, ca.fetchControllers().size());
            assertEquals(0, ca.fetchBridgeServers().size());
          }
      });
    vm3.invoke(stopLocator);
  }
}
