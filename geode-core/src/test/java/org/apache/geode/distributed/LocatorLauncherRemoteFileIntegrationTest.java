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

import org.apache.geode.distributed.LocatorLauncher.Builder;
import org.apache.geode.internal.process.ProcessControllerFactory;
import org.apache.geode.lang.AttachAPINotFoundException;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * Integration tests for using {@code LocatorLauncher} as an application main in a forked JVM
 * without the Attach API.
 *
 * Sets {@link ProcessControllerFactory#PROPERTY_DISABLE_ATTACH_API} to force
 * {@code LocatorLauncher} to use the FileProcessController implementation.
 *
 * @since GemFire 8.0
 */
@Category(IntegrationTest.class)
public class LocatorLauncherRemoteFileIntegrationTest extends LocatorLauncherRemoteIntegrationTest {

  @Before
  public void setUpLocatorLauncherRemoteFileIntegrationTest() throws Exception {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isFalse();
  }

  @Test
  @Override
  public void statusWithPidReturnsOnlineWithDetails() throws Exception {
    givenRunningLocator();

    assertThatThrownBy(() -> new Builder().setPid(getLocatorPid()).build().status())
        .isInstanceOf(AttachAPINotFoundException.class);
  }

  @Test
  @Override
  public void stopWithPidDeletesPidFile() throws Exception {
    givenRunningLocator();

    assertThatThrownBy(() -> new Builder().setPid(getLocatorPid()).build().stop())
        .isInstanceOf(AttachAPINotFoundException.class);
  }

  @Test
  @Override
  public void stopWithPidReturnsStopped() throws Exception {
    givenRunningLocator();

    assertThatThrownBy(() -> new Builder().setPid(getLocatorPid()).build().stop())
        .isInstanceOf(AttachAPINotFoundException.class);
  }

  @Test
  @Override
  public void stopWithPidStopsLocatorProcess() throws Exception {
    givenRunningLocator();

    assertThatThrownBy(() -> new Builder().setPid(getLocatorPid()).build().stop())
        .isInstanceOf(AttachAPINotFoundException.class);
  }
}
