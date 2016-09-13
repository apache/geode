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
package org.apache.geode.distributed;

import org.apache.geode.distributed.AbstractLauncher.Status;
import org.apache.geode.distributed.ServerLauncher.ServerState;
import org.apache.geode.distributed.internal.DistributionConfig;
import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.internal.cache.AbstractCacheServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ErrorCollector;
import org.junit.rules.TemporaryFolder;

import java.util.concurrent.Callable;

import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.junit.Assert.assertNotNull;

/**
 * @since GemFire 8.0
 */
public abstract class AbstractServerLauncherIntegrationTestCase extends AbstractLauncherIntegrationTestCase {
  
  protected volatile int serverPort;
  protected volatile ServerLauncher launcher;
  protected volatile String workingDirectory;

  @Rule
  public ErrorCollector errorCollector= new ErrorCollector();
  
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public final void setUpServerLauncherTest() throws Exception {
    System.setProperty(DistributionConfig.GEMFIRE_PREFIX + MCAST_PORT, Integer.toString(0));
    final int port = AvailablePortHelper.getRandomAvailableTCPPort();
    System.setProperty(AbstractCacheServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY, String.valueOf(port));
    this.serverPort = port;
    this.workingDirectory = this.temporaryFolder.getRoot().getCanonicalPath();
  }

  @After
  public final void tearDownServerLauncherTest() throws Exception {    
    this.serverPort = 0;
    if (this.launcher != null) {
      this.launcher.stop();
      this.launcher = null;
    }
  }
  
  protected void waitForServerToStart(final ServerLauncher launcher, int timeout, int interval, boolean throwOnTimeout) throws Exception {
    assertEventuallyTrue("waiting for local Server to start: " + launcher.status(), new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          final ServerState serverState = launcher.status();
          assertNotNull(serverState);
          return Status.ONLINE.equals(serverState.getStatus());
        }
        catch (RuntimeException e) {
          return false;
        }
      }
    }, timeout, interval);
  }

  protected void waitForServerToStart(final ServerLauncher launcher, boolean throwOnTimeout) throws Exception {
    waitForServerToStart(launcher, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForServerToStart(final ServerLauncher launcher, int timeout, boolean throwOnTimeout) throws Exception {
    waitForServerToStart(launcher, timeout, INTERVAL_MILLISECONDS, throwOnTimeout);
  }
  
  protected void waitForServerToStart(final ServerLauncher launcher) throws Exception {
    waitForServerToStart(launcher, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS, true);
  }
}
