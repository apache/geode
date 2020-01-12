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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import org.junit.Before;
import org.junit.Test;

import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.lang.AttachAPINotFoundException;

/**
 * Subclass of {@link ServerLauncherLocalIntegrationTest} which forces the code to not find the
 * Attach API. As a result {@link ServerLauncher} ends up using the FileProcessController
 * implementation.
 *
 * @since GemFire 8.0
 */
public class ServerLauncherRemoteFileIntegrationTest extends ServerLauncherRemoteIntegrationTest {

  @Before
  public void setUpServerLauncherRemoteFileTest() {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isFalse();
  }

  /**
   * Override to assert that STATUS with --pid throws AttachAPINotFoundException
   */
  @Override
  @Test
  public void statusWithPidReturnsOnlineWithDetails() {
    givenRunningServer();

    Throwable thrown = catchThrowable(() -> new Builder()
        .setPid(getServerPid())
        .build()
        .status());

    assertThat(thrown)
        .isInstanceOf(AttachAPINotFoundException.class);
  }

  /**
   * Override to assert that STOP with --pid throws AttachAPINotFoundException
   */
  @Override
  @Test
  public void stopWithPidDeletesPidFile() {
    givenRunningServer();

    Throwable thrown = catchThrowable(() -> new Builder()
        .setPid(getServerPid())
        .build()
        .stop());

    assertThat(thrown)
        .isInstanceOf(AttachAPINotFoundException.class);
  }

  /**
   * Override to assert that STOP with --pid throws AttachAPINotFoundException
   */
  @Override
  @Test
  public void stopWithPidReturnsStopped() {
    givenRunningServer();

    Throwable thrown = catchThrowable(() -> new Builder()
        .setPid(getServerPid())
        .build()
        .stop());

    assertThat(thrown)
        .isInstanceOf(AttachAPINotFoundException.class);
  }

  /**
   * Override to assert that STOP with --pid throws AttachAPINotFoundException
   */
  @Override
  @Test
  public void stopWithPidStopsServerProcess() {
    givenRunningServer();

    Throwable thrown = catchThrowable(() -> new Builder()
        .setPid(getServerPid())
        .build()
        .stop());

    assertThat(thrown)
        .isInstanceOf(AttachAPINotFoundException.class);
  }
}
