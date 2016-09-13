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

import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.process.ProcessStreamReader;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public abstract class AbstractServerLauncherRemoteIntegrationTestCase extends AbstractServerLauncherIntegrationTestCase {

  protected volatile Process process;
  protected volatile ProcessStreamReader processOutReader;
  protected volatile ProcessStreamReader processErrReader;

  @Before
  public final void setUpAbstractServerLauncherRemoteIntegrationTestCase() throws Exception {
  }

  @After
  public final void tearDownAbstractServerLauncherRemoteIntegrationTestCase() throws Exception {
    if (this.process != null) {
      this.process.destroy();
      this.process = null;
    }
    if (this.processOutReader != null && this.processOutReader.isRunning()) {
      this.processOutReader.stop();
    }
    if (this.processErrReader != null && this.processErrReader.isRunning()) {
      this.processErrReader.stop();
    }
  }

  /**
   * Override as needed.
   */
  protected List<String> getJvmArguments() {
    final List<String> jvmArguments = new ArrayList<String>();
    jvmArguments.add("-D" + DistributionConfig.GEMFIRE_PREFIX + "log-level=config");
    return jvmArguments;
  }

  /**
   * Remove final if a test needs to override.
   */
  protected final AbstractLauncher.Status getExpectedStopStatusForNotRunning() {
    return AbstractLauncher.Status.NOT_RESPONDING;
  }

  protected void waitForServerToStart() throws Exception {
    assertEventuallyTrue("waiting for local Server to start: " + launcher.status(), new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        try {
          assertNotNull(process);
          try {
            final int value = process.exitValue();
            fail("Process has died with exit value " + value + " while waiting for it to start.");
          } catch (IllegalThreadStateException e) {
            // expected
          }
          final ServerLauncher.ServerState serverState = launcher.status();
          assertNotNull(serverState);
          logger.info("serverState: " + serverState);
          return AbstractLauncher.Status.ONLINE.equals(serverState.getStatus());
        } catch (RuntimeException e) {
          logger.error(e, e);
          return false;
        }
      }
    }, TIMEOUT_MILLISECONDS, INTERVAL_MILLISECONDS);
  }

}
