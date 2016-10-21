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
package org.apache.geode.distributed;

import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.distributed.internal.DistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.net.SocketCreator;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.dunit.Assert.*;
import static org.apache.geode.test.dunit.Host.getHost;

/**
 * Extracted from LocatorLauncherLocalIntegrationTest.
 * 
 * @since GemFire 8.0
 */
@Category(DistributedTest.class)
public class HostedLocatorsDUnitTest extends JUnit4DistributedTestCase {

  protected static final int TIMEOUT_MILLISECONDS = 5 * 60 * 1000; // 5 minutes

  protected transient volatile int locatorPort;
  protected transient volatile LocatorLauncher launcher;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void preTearDown() throws Exception {
    disconnectAllFromDS();
  }

  @Test
  public void testGetAllHostedLocators() throws Exception {
    final InternalDistributedSystem system = getSystem();
    final String dunitLocator = system.getConfig().getLocators();
    assertNotNull(dunitLocator);
    assertFalse(dunitLocator.isEmpty());

    final int[] ports = getRandomAvailableTCPPorts(4);

    final String uniqueName = getUniqueName();
    for (int i = 0; i < 4; i++) {
      final int whichvm = i;
      getHost(0).getVM(whichvm).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          try {
            System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "locators", dunitLocator);
            System.setProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT, "0");

            final String name = uniqueName + "-" + whichvm;
            final File subdir = new File(name);
            subdir.mkdir();
            assertTrue(subdir.exists() && subdir.isDirectory());

            final Builder builder = new Builder().setMemberName(name).setPort(ports[whichvm])
                .setRedirectOutput(true).setWorkingDirectory(name);

            launcher = builder.build();
            assertEquals(Status.ONLINE, launcher.start().getStatus());
            waitForLocatorToStart(launcher, TIMEOUT_MILLISECONDS, 10, true);
            return null;
          } finally {
            System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "locators");
            System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT);
          }
        }
      });
    }

    final String host = SocketCreator.getLocalHost().getHostAddress();

    final Set<String> locators = new HashSet<String>();
    locators.add(host + "["
        + dunitLocator.substring(dunitLocator.indexOf("[") + 1, dunitLocator.indexOf("]")) + "]");
    for (int port : ports) {
      locators.add(host + "[" + port + "]");
    }

    // validation within non-locator
    final DistributionManager dm = (DistributionManager) system.getDistributionManager();

    final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
    assertEquals(5, locatorIds.size());

    final Map<InternalDistributedMember, Collection<String>> hostedLocators =
        dm.getAllHostedLocators();
    assertTrue(!hostedLocators.isEmpty());
    assertEquals(5, hostedLocators.size());

    for (InternalDistributedMember member : hostedLocators.keySet()) {
      assertEquals(1, hostedLocators.get(member).size());
      final String hostedLocator = hostedLocators.get(member).iterator().next();
      assertTrue(locators + " does not contain " + hostedLocator, locators.contains(hostedLocator));
    }

    // validate fix for #46324
    for (int whichvm = 0; whichvm < 4; whichvm++) {
      getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final DistributionManager dm = (DistributionManager) InternalDistributedSystem
              .getAnyInstance().getDistributionManager();
          final InternalDistributedMember self = dm.getDistributionManagerId();

          final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
          assertTrue(locatorIds.contains(self));

          final Map<InternalDistributedMember, Collection<String>> hostedLocators =
              dm.getAllHostedLocators();
          assertTrue(
              "hit bug #46324: " + hostedLocators + " is missing "
                  + InternalLocator.getLocatorStrings() + " for " + self,
              hostedLocators.containsKey(self));
        }
      });
    }

    // validation with locators
    for (int whichvm = 0; whichvm < 4; whichvm++) {
      getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final DistributionManager dm = (DistributionManager) InternalDistributedSystem
              .getAnyInstance().getDistributionManager();

          final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
          assertEquals(5, locatorIds.size());

          final Map<InternalDistributedMember, Collection<String>> hostedLocators =
              dm.getAllHostedLocators();
          assertTrue(!hostedLocators.isEmpty());
          assertEquals(5, hostedLocators.size());

          for (InternalDistributedMember member : hostedLocators.keySet()) {
            assertEquals(1, hostedLocators.get(member).size());
            final String hostedLocator = hostedLocators.get(member).iterator().next();
            assertTrue(locators + " does not contain " + hostedLocator,
                locators.contains(hostedLocator));
          }
        }
      });
    }
  }

  @Test
  public void testGetAllHostedLocatorsUsingPortZero() throws Exception {
    final InternalDistributedSystem system = getSystem();
    final String dunitLocator = system.getConfig().getLocators();
    assertNotNull(dunitLocator);
    assertFalse(dunitLocator.isEmpty());

    // This will eventually contain the ports used by locators
    final int[] ports = new int[] {0, 0, 0, 0};

    final String uniqueName = getUniqueName();
    for (int i = 0; i < 4; i++) {
      final int whichvm = i;
      Integer port = (Integer) Host.getHost(0).getVM(whichvm).invoke(new SerializableCallable() {
        @Override
        public Object call() throws Exception {
          try {
            System.setProperty(DistributionConfig.GEMFIRE_PREFIX + "locators", dunitLocator);
            System.setProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT, "0");

            final String name = uniqueName + "-" + whichvm;
            final File subdir = new File(name);
            subdir.mkdir();
            assertTrue(subdir.exists() && subdir.isDirectory());

            final Builder builder = new Builder().setMemberName(name).setPort(ports[whichvm])
                .setRedirectOutput(true).setWorkingDirectory(name);

            launcher = builder.build();
            assertEquals(Status.ONLINE, launcher.start().getStatus());
            waitForLocatorToStart(launcher, TIMEOUT_MILLISECONDS, 10, true);
            return launcher.getPort();
          } finally {
            System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + "locators");
            System.clearProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT);
          }
        }
      });
      ports[i] = port;
    }

    final String host = SocketCreator.getLocalHost().getHostAddress();

    final Set<String> locators = new HashSet<String>();
    locators.add(host + "["
        + dunitLocator.substring(dunitLocator.indexOf("[") + 1, dunitLocator.indexOf("]")) + "]");
    for (int port : ports) {
      locators.add(host + "[" + port + "]");
    }

    // validation within non-locator
    final DistributionManager dm = (DistributionManager) system.getDistributionManager();

    final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
    assertEquals(5, locatorIds.size());

    final Map<InternalDistributedMember, Collection<String>> hostedLocators =
        dm.getAllHostedLocators();
    assertTrue(!hostedLocators.isEmpty());
    assertEquals(5, hostedLocators.size());

    for (InternalDistributedMember member : hostedLocators.keySet()) {
      assertEquals(1, hostedLocators.get(member).size());
      final String hostedLocator = hostedLocators.get(member).iterator().next();
      assertTrue(locators + " does not contain " + hostedLocator, locators.contains(hostedLocator));
    }

    // validate fix for #46324
    for (int whichvm = 0; whichvm < 4; whichvm++) {
      Host.getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final DistributionManager dm = (DistributionManager) InternalDistributedSystem
              .getAnyInstance().getDistributionManager();
          final InternalDistributedMember self = dm.getDistributionManagerId();

          final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
          assertTrue(locatorIds.contains(self));

          final Map<InternalDistributedMember, Collection<String>> hostedLocators =
              dm.getAllHostedLocators();
          assertTrue(
              "hit bug #46324: " + hostedLocators + " is missing "
                  + InternalLocator.getLocatorStrings() + " for " + self,
              hostedLocators.containsKey(self));
        }
      });
    }

    // validation with locators
    for (int whichvm = 0; whichvm < 4; whichvm++) {
      Host.getHost(0).getVM(whichvm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final DistributionManager dm = (DistributionManager) InternalDistributedSystem
              .getAnyInstance().getDistributionManager();

          final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
          assertEquals(5, locatorIds.size());

          final Map<InternalDistributedMember, Collection<String>> hostedLocators =
              dm.getAllHostedLocators();
          assertTrue(!hostedLocators.isEmpty());
          assertEquals(5, hostedLocators.size());

          for (InternalDistributedMember member : hostedLocators.keySet()) {
            assertEquals(1, hostedLocators.get(member).size());
            final String hostedLocator = hostedLocators.get(member).iterator().next();
            assertTrue(locators + " does not contain " + hostedLocator,
                locators.contains(hostedLocator));
          }
        }
      });
    }
  }

  protected void waitForLocatorToStart(final LocatorLauncher launcher, int timeout, int interval,
      boolean throwOnTimeout) throws Exception {
    assertEventuallyTrue("waiting for process to start: " + launcher.status(),
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            try {
              final LocatorState LocatorState = launcher.status();
              return (LocatorState != null && Status.ONLINE.equals(LocatorState.getStatus()));
            } catch (RuntimeException e) {
              return false;
            }
          }
        }, timeout, interval);
  }

  protected static void assertEventuallyTrue(final String message, final Callable<Boolean> callable,
      final int timeout, final int interval) throws Exception {
    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < timeout; done =
        (callable.call())) {
      Thread.sleep(interval);
    }
    assertTrue(message, done);
  }
}
