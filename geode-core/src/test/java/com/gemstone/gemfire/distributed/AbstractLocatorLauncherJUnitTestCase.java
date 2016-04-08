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
package com.gemstone.gemfire.distributed;

import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;

import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.LocatorLauncher.Builder;
import com.gemstone.gemfire.distributed.LocatorLauncher.LocatorState;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.DistributionLocator;

/**
 * @since 8.0
 */
public abstract class AbstractLocatorLauncherJUnitTestCase extends AbstractLauncherJUnitTestCase {

  protected volatile int locatorPort;
  protected volatile LocatorLauncher launcher;
  
  @Rule
  public ErrorCollector errorCollector = new ErrorCollector();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public final void setUpLocatorLauncherTest() throws Exception {
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    System.setProperty(DistributionLocator.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, String.valueOf(port));
    this.locatorPort = port;
  }
  
  @After
  public final void tearDownLocatorLauncherTest() throws Exception {    
    this.locatorPort = 0;
    if (this.launcher != null) {
      this.launcher.stop();
      this.launcher = null;
    }
  }
  
  protected void waitForLocatorToStart(final LocatorLauncher launcher, int timeout, int interval, boolean throwOnTimeout) throws Exception {
    assertEventuallyTrue("waiting for process to start: " + launcher.status(), new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          final LocatorState LocatorState = launcher.status();
          return (LocatorState != null && Status.ONLINE.equals(LocatorState.getStatus()));
        }
        catch (RuntimeException e) {
          return false;
        }
      }
    }, timeout, interval);
  }
  
  protected void waitForLocatorToStart(final LocatorLauncher launcher, int timeout, boolean throwOnTimeout) throws Exception {
    waitForLocatorToStart(launcher, timeout, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForLocatorToStart(final LocatorLauncher launcher, boolean throwOnTimeout) throws Exception {
    waitForLocatorToStart(launcher, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForLocatorToStart(final LocatorLauncher launcher) throws Exception {
    waitForLocatorToStart(launcher, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
  }
  
  protected static void waitForLocatorToStart(int port, int timeout, int interval, boolean throwOnTimeout) throws Exception {
    final LocatorLauncher locatorLauncher = new Builder().setPort(port).build();
    assertEventuallyTrue("Waiting for Locator in other process to start.", new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          final LocatorState locatorState = locatorLauncher.status();
          return (locatorState != null && Status.ONLINE.equals(locatorState.getStatus()));
        }
        catch (RuntimeException e) {
          return false;
        }
      }
    }, timeout, interval);
  }
}
