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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.internal.AvailablePortHelper.getRandomAvailableTCPPorts;
import static org.apache.geode.test.dunit.Host.getHost;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.shell.support.util.FileUtils;

import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.distributed.LocatorLauncher.LocatorState;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.inet.LocalHostUtil;
import org.apache.geode.internal.util.StopWatch;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.SerializableRunnable;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.ClientServerTest;

/**
 * Extracted from LocatorLauncherLocalIntegrationTest.
 *
 * @since GemFire 8.0
 */
@Category({ClientServerTest.class})
public class HostedLocatorsDUnitTest extends JUnit4DistributedTestCase {

  private static final int TIMEOUT_MILLISECONDS = (int) MINUTES.toMillis(5);

  private transient volatile LocatorLauncher launcher;

  @Override
  public final void postSetUp() throws Exception {
    disconnectAllFromDS();
  }

  @Override
  public final void preTearDown() throws Exception {
    disconnectAllFromDS();
  }

  private String getUniqueLocatorName() {
    return getHost(0).getHostName() + "_" + getUniqueName();
  }

  @Test
  public void testGetAllHostedLocators() throws Exception {
    final InternalDistributedSystem system = getSystem();
    final String dunitLocator = system.getConfig().getLocators();
    assertThat(dunitLocator).isNotEmpty();

    final int[] ports = getRandomAvailableTCPPorts(4);

    final String uniqueName = getUniqueLocatorName();
    for (int whichVm = 0; whichVm < 4; whichVm++) {
      getHost(0).getVM(whichVm)
          .invoke(new LocatorStarter(uniqueName, whichVm, ports, dunitLocator));
    }

    validateLocators(dunitLocator, ports, system);
  }

  @Test
  public void testGetAllHostedLocatorsUsingPortZero() throws Exception {
    final InternalDistributedSystem system = getSystem();
    final String dunitLocator = system.getConfig().getLocators();
    assertThat(dunitLocator).isNotEmpty();

    // This will eventually contain the ports used by locators
    final int[] ports = new int[] {0, 0, 0, 0};

    final String uniqueName = getUniqueLocatorName();
    for (int whichVm = 0; whichVm < 4; whichVm++) {
      ports[whichVm] = (Integer) getHost(0).getVM(whichVm).invoke(
          new LocatorStarter(uniqueName, whichVm, ports, dunitLocator));
    }

    validateLocators(dunitLocator, ports, system);
  }

  private void validateLocators(final String dunitLocator, final int[] ports,
      final InternalDistributedSystem system) throws UnknownHostException {
    final String host = LocalHostUtil.getLocalHost().getHostAddress();

    final Set<String> locators = new HashSet<>();
    locators.add(host + "["
        + dunitLocator.substring(dunitLocator.indexOf("[") + 1, dunitLocator.indexOf("]")) + "]");
    for (int port : ports) {
      locators.add(host + "[" + port + "]");
    }

    // validation within non-locator
    final ClusterDistributionManager dm =
        (ClusterDistributionManager) system.getDistributionManager();

    validateWithLocators(dm, locators);

    for (int whichVm = 0; whichVm < 4; whichVm++) {
      getHost(0).getVM(whichVm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final ClusterDistributionManager dm =
              (ClusterDistributionManager) InternalDistributedSystem.getAnyInstance()
                  .getDistributionManager();
          final InternalDistributedMember self = dm.getDistributionManagerId();

          final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
          assertThat(locatorIds).contains(self);

          final Map<InternalDistributedMember, Collection<String>> hostedLocators =
              dm.getAllHostedLocators();
          assertThat(hostedLocators).containsKey(self);
        }
      });
    }

    for (int whichVm = 0; whichVm < 4; whichVm++) {
      getHost(0).getVM(whichVm).invoke(new SerializableRunnable() {
        @Override
        public void run() {
          final ClusterDistributionManager dm1 =
              (ClusterDistributionManager) InternalDistributedSystem.getAnyInstance()
                  .getDistributionManager();

          validateWithLocators(dm1, locators);
        }
      });
    }
  }

  private void validateWithLocators(final ClusterDistributionManager dm,
      final Set<String> locators) {
    final Set<InternalDistributedMember> locatorIds = dm.getLocatorDistributionManagerIds();
    assertThat(locatorIds).hasSize(5);

    final Map<InternalDistributedMember, Collection<String>> hostedLocators =
        dm.getAllHostedLocators();
    assertThat(hostedLocators).hasSize(5);

    for (InternalDistributedMember member : hostedLocators.keySet()) {
      final Collection<String> hostedLocator = hostedLocators.get(member);
      assertThat(hostedLocator).hasSize(1);
      assertThat(locators).containsAll(hostedLocator);
    }
  }

  private void waitForLocatorToStart(final LocatorLauncher launcher, int timeout, int interval)
      throws Exception {
    assertEventuallyTrue("waiting for process to start: " + launcher.status(),
        () -> {
          try {
            final LocatorState LocatorState = launcher.status();
            return (LocatorState != null && Status.ONLINE.equals(LocatorState.getStatus()));
          } catch (RuntimeException e) {
            return false;
          }
        }, timeout, interval);
  }

  private static void assertEventuallyTrue(final String message, final Callable<Boolean> callable,
      final int timeout, final int interval) throws Exception {
    boolean done = false;
    for (StopWatch time = new StopWatch(true); !done && time.elapsedTimeMillis() < timeout; done =
        (callable.call())) {
      Thread.sleep(interval);
    }
    assertThat(done).as(message).isTrue();
  }

  private class LocatorStarter extends SerializableCallable<Object> {
    private final String uniqueName;
    private final int whichVm;
    private final int[] ports;
    private final String dunitLocator;

    public LocatorStarter(final String uniqueName, final int whichVm, final int[] ports,
        final String dunitLocator) {
      this.uniqueName = uniqueName;
      this.whichVm = whichVm;
      this.ports = ports;
      this.dunitLocator = dunitLocator;
    }

    @Override
    public Object call() throws Exception {
      final String name = uniqueName + "-" + whichVm;
      final File directory = new File(name);
      if (directory.exists()) {
        FileUtils.deleteRecursively(directory);
      }
      directory.mkdir();
      assertThat(directory).exists().isDirectory();

      final Builder builder = new Builder().setMemberName(name).setPort(ports[whichVm])
          .set(LOCATORS, dunitLocator)
          .setRedirectOutput(true).setWorkingDirectory(name);

      launcher = builder.build();
      assertThat(launcher.start().getStatus()).isSameAs(Status.ONLINE);
      waitForLocatorToStart(launcher, TIMEOUT_MILLISECONDS, 10);
      return launcher.getPort();
    }
  }
}
