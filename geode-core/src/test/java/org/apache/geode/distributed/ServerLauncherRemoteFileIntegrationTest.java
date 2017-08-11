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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.ServerLauncher.Builder;
import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.lang.AttachAPINotFoundException;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Subclass of {@link ServerLauncherLocalIntegrationTest} which forces the code to not find the
 * Attach API. As a result {@link ServerLauncher} ends up using the FileProcessController
 * implementation.
 * 
 * @since GemFire 8.0
 */
@Category(IntegrationTest.class)
public class ServerLauncherRemoteFileIntegrationTest extends ServerLauncherRemoteIntegrationTest {

  @Before
  public void setUpServerLauncherRemoteFileTest() throws Exception {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isFalse();
  }

  /**
   * Override to assert that STATUS with --pid throws AttachAPINotFoundException
   */
  @Override
  @Test
  public void statusWithPidReturnsOnlineWithDetails() throws Exception {
    givenRunningServer();

    assertThatThrownBy(() -> new Builder().setPid(getServerPid()).build().status())
        .isInstanceOf(AttachAPINotFoundException.class);
  }

  /**
   * Override to assert that STOP with --pid throws AttachAPINotFoundException
   */
  @Override
  @Test
  public void stopWithPidDeletesPidFile() throws Exception {
    givenRunningServer();

    assertThatThrownBy(() -> new Builder().setPid(getServerPid()).build().stop())
        .isInstanceOf(AttachAPINotFoundException.class);
  }

  /**
   * Override to assert that STOP with --pid throws AttachAPINotFoundException
   */
  @Override
  @Test
  public void stopWithPidReturnsStopped() throws Exception {
    givenRunningServer();

    assertThatThrownBy(() -> new Builder().setPid(getServerPid()).build().stop())
        .isInstanceOf(AttachAPINotFoundException.class);
  }

  /**
   * Override to assert that STOP with --pid throws AttachAPINotFoundException
   */
  @Override
  @Test
  public void stopWithPidStopsServerProcess() throws Exception {
    givenRunningServer();

    assertThatThrownBy(() -> new Builder().setPid(getServerPid()).build().stop())
        .isInstanceOf(AttachAPINotFoundException.class);
  }
}
