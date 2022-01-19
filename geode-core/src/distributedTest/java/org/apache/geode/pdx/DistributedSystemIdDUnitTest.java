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
package org.apache.geode.pdx;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.START_LOCATOR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.IncompatibleSystemException;
import org.apache.geode.distributed.internal.ClusterDistributionManager;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.IgnoredException;
import org.apache.geode.test.dunit.SerializableCallable;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.internal.JUnit4DistributedTestCase;
import org.apache.geode.test.junit.categories.SerializationTest;

@Category({SerializationTest.class})
public class DistributedSystemIdDUnitTest extends JUnit4DistributedTestCase {

  @Override
  public void preSetUp() {
    disconnectAllFromDS(); // GEODE-558 test fails due to infection from another test
  }

  @Test
  public void testMatchingIds() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);
    VM vm2 = host.getVM(2);

    int locatorPort = createLocator(vm0, "1");
    createSystem(vm1, "1", locatorPort);
    createSystem(vm2, "1", locatorPort);

    checkId(vm0, 1);
    checkId(vm1, 1);
    checkId(vm2, 1);
  }

  @Test
  public void testInfectiousId() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    int locatorPort = createLocator(vm0, "1");
    createSystem(vm1, "-1", locatorPort);

    checkId(vm1, 1);
  }

  @Test
  public void testMismatch() {
    IgnoredException.addIgnoredException("Rejected new system node");
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    int locatorPort = createLocator(vm0, "1");

    // Creating a member with a different distributed system id should fail
    assertThatThrownBy(() -> createSystem(vm1, "2", locatorPort))
        .hasCauseInstanceOf(IncompatibleSystemException.class);

    checkId(vm0, 1);
  }

  @Test
  public void testInvalid() {
    Host host = Host.getHost(0);
    VM vm0 = host.getVM(0);
    VM vm1 = host.getVM(1);

    try {
      createLocator(vm0, "256");
      fail("Should have gotten an exception");
    } catch (Exception ignored) {
    }

    try {
      createLocator(vm0, "aardvark");
      fail("Should have gotten an exception");
    } catch (Exception ignored) {
    }
  }

  private void createSystem(VM vm, final String dsId, final int locatorPort) {

    SerializableCallable createSystem = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        Properties props = new Properties();
        props.setProperty(DISTRIBUTED_SYSTEM_ID, dsId);
        props.setProperty(LOCATORS, "localhost[" + locatorPort + "]");
        getSystem(props);
        return null;
      }
    };
    vm.invoke(createSystem);
  }

  private int createLocator(VM vm, final String dsId) {
    SerializableCallable createSystem = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        int port = AvailablePortHelper.getRandomAvailableTCPPort();
        Properties props = new Properties();
        props.setProperty(DISTRIBUTED_SYSTEM_ID, dsId);
        props.setProperty(MCAST_PORT, "0");
        props.setProperty(LOCATORS, "localhost[" + port + "]");
        props.setProperty(START_LOCATOR, "localhost[" + port + "]");
        getSystem(props);
        return port;
      }
    };
    return (Integer) vm.invoke(createSystem);
  }

  private void checkId(VM vm, final int dsId) {

    SerializableCallable createSystem = new SerializableCallable() {
      @Override
      public Object call() throws Exception {
        ClusterDistributionManager dm =
            (ClusterDistributionManager) basicGetSystem().getDistributionManager();
        assertEquals(dsId, dm.getDistributedSystemId());
        return null;
      }
    };
    vm.invoke(createSystem);
  }
}
