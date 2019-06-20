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

import org.junit.Before;

import org.apache.geode.internal.process.ProcessControllerFactory;

/**
 * Integration tests for using {@link LocatorLauncher} as an in-process API within an existing JVM
 * without the Attach API.
 *
 * Sets {@link ProcessControllerFactory#PROPERTY_DISABLE_ATTACH_API} to force
 * {@code LocatorLauncher} to use the FileProcessController implementation.
 *
 * @since GemFire 8.0
 */
public class LocatorLauncherLocalFileIntegrationTest extends LocatorLauncherLocalIntegrationTest {

  @Before
  public void setUpLocatorLauncherLocalFileIntegrationTest() {
    System.setProperty(ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API, "true");
    assertThat(new ProcessControllerFactory().isAttachAPIFound()).isFalse();
  }
}
