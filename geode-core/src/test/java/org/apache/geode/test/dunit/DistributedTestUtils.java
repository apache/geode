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
package org.apache.geode.test.dunit;

import static org.apache.geode.distributed.ConfigurationProperties.DISABLE_AUTO_RECONNECT;
import static org.apache.geode.test.dunit.VM.getHostName;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.membership.gms.MembershipManagerHelper;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.InternalInstantiator;

/**
 * {@code DistributedTestUtils} provides static utility methods that affect the runtime environment
 * or artifacts generated by a DistributedTest.
 *
 * <p>
 * These methods can be used directly: {@code DistributedTestUtils.crashDistributedSystem(...)},
 * however, they are intended to be referenced through static import:
 *
 * <pre>
 * import static org.apache.geode.test.dunit.DistributedTestUtils.*;
 *    ...
 *    crashDistributedSystem(...);
 * </pre>
 *
 * Extracted from DistributedTestCase.
 */
public class DistributedTestUtils {

  protected DistributedTestUtils() {
    // nothing
  }

  /**
   * Fetches the GemFireDescription for this test and adds its DistributedSystem properties to the
   * provided props parameter.
   *
   * @param properties the properties to add hydra's test properties to
   */
  public static void addHydraProperties(final Properties properties) {
    Properties dsProperties = DUnitEnv.get().getDistributedSystemProperties();
    for (Iterator<Map.Entry<Object, Object>> iter = dsProperties.entrySet().iterator(); iter
        .hasNext();) {
      Map.Entry<Object, Object> entry = iter.next();
      String key = (String) entry.getKey();
      String value = (String) entry.getValue();
      if (properties.getProperty(key) == null) {
        properties.setProperty(key, value);
      }
    }
  }

  /**
   * Crash the cache in the given VM in such a way that it immediately stops communicating with
   * peers. This forces the VM's membership manager to throw a ForcedDisconnectException by forcibly
   * terminating the JGroups protocol stack with a fake EXIT event.
   *
   * <p>
   * NOTE: if you use this method be sure that you clean up the VM before the end of your test with
   * disconnectFromDS() or disconnectAllFromDS().
   */
  public static void crashDistributedSystem(final DistributedSystem system) {
    MembershipManagerHelper.crashDistributedSystem(system);
    MembershipManagerHelper.inhibitForcedDisconnectLogging(false);
    WaitCriterion wc = new WaitCriterion() {
      public boolean done() {
        return !system.isConnected();
      }

      public String description() {
        return "Waiting for distributed system to finish disconnecting: " + system;
      }
    };
    Wait.waitForCriterion(wc, 10000, 1000, true);
  }

  /**
   * Crash the cache in the given VM in such a way that it immediately stops communicating with
   * peers. This forces the VM's membership manager to throw a ForcedDisconnectException by forcibly
   * terminating the JGroups protocol stack with a fake EXIT event.
   *
   * <p>
   * NOTE: if you use this method be sure that you clean up the VM before the end of your test with
   * disconnectFromDS() or disconnectAllFromDS().
   */
  public static boolean crashDistributedSystem(final VM vm) {
    return vm.invoke(() -> {
      DistributedSystem system = InternalDistributedSystem.getAnyInstance();
      crashDistributedSystem(system);
      return true;
    });
  }

  /**
   * Delete locator state files. Use this after getting a random port to ensure that an old locator
   * state file isn't picked up by the new locator you're starting.
   */
  public static void deleteLocatorStateFile(final int... ports) {
    for (int index = 0; index < ports.length; index++) {
      File stateFile = new File("locator" + ports[index] + "view.dat");
      if (stateFile.exists()) {
        stateFile.delete();
      }
    }
  }

  public static Properties getAllDistributedSystemProperties(final Properties properties) {
    Properties dsProperties = DUnitEnv.get().getDistributedSystemProperties();

    // our tests do not expect auto-reconnect to be on by default
    if (!dsProperties.contains(DISABLE_AUTO_RECONNECT)) {
      dsProperties.put(DISABLE_AUTO_RECONNECT, "true");
    }

    for (Iterator<Map.Entry<Object, Object>> iterator = properties.entrySet().iterator(); iterator
        .hasNext();) {
      Map.Entry<Object, Object> entry = iterator.next();
      String key = (String) entry.getKey();
      Object value = entry.getValue();
      dsProperties.put(key, value);
    }
    System.out.println("distributed system properties: " + dsProperties);
    return dsProperties;
  }

  /**
   * Returns the port that the standard dunit locator is listening on.
   *
   * @deprecated Please use {@link #getLocatorPort()} instead.
   */
  @Deprecated
  public static int getDUnitLocatorPort() {
    return getLocatorPort();
  }

  /**
   * Returns the port that the standard dunit locator is listening on.
   */
  public static int getLocatorPort() {
    return DUnitEnv.get().getLocatorPort();
  }

  /**
   * Returns a {@link ConfigurationProperties#LOCATORS} string for the standard dunit locator.
   */
  public static String getLocators() {
    return getHostName() + "[" + getLocatorPort() + "]";
  }

  public static void unregisterAllDataSerializersFromAllVms() {
    DistributedTestUtils.unregisterDataSerializerInThisVM();
    Invoke.invokeInEveryVM(() -> unregisterDataSerializerInThisVM());
    Invoke.invokeInLocator(() -> unregisterDataSerializerInThisVM());
  }

  public static void unregisterDataSerializerInThisVM() {
    // TODO: delete DataSerializerPropogationDUnitTest.successfullyLoadedTestDataSerializer = false;
    // unregister all the Dataserializers
    InternalDataSerializer.reinitialize();
    // ensure that all are unregistered
    assertEquals(0, InternalDataSerializer.getSerializers().length);
  }

  public static void unregisterInstantiatorsInThisVM() {
    // unregister all the instantiators
    InternalInstantiator.reinitialize();
    assertEquals(0, InternalInstantiator.getInstantiators().length);
  }
}
