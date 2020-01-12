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
package org.apache.geode.internal.process;

import static org.apache.geode.internal.process.ProcessControllerFactory.PROPERTY_DISABLE_ATTACH_API;
import static org.apache.geode.internal.process.ProcessUtils.identifyPid;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;


/**
 * Unit tests for {@link ProcessControllerFactory}.
 *
 * @since GemFire 8.0
 */
public class ProcessControllerFactoryTest {

  private ProcessControllerFactory factory;
  private ProcessControllerParameters parameters;
  private int pid;

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  @Before
  public void before() throws Exception {
    factory = new ProcessControllerFactory();
    parameters = mock(ProcessControllerParameters.class);
    pid = identifyPid();
  }

  @Test
  public void isAttachAPIFound_withAttachAPI_returnsTrue() throws Exception {
    // act/assert
    assertThat(factory.isAttachAPIFound()).isTrue();
  }

  @Test
  public void isAttachAPIFound_withoutAttachAPI_returnsFalse() throws Exception {
    // arrange
    System.setProperty(PROPERTY_DISABLE_ATTACH_API, "true");
    factory = new ProcessControllerFactory();

    // act/assert
    assertThat(factory.isAttachAPIFound()).isFalse();
  }

  @Test
  public void createProcessController_withAttachAPI_returnsMBeanProcessController()
      throws Exception {
    // act
    ProcessController controller = factory.createProcessController(parameters, pid);

    // assert
    assertThat(controller).isInstanceOf(MBeanOrFileProcessController.class);
  }

  @Test
  public void createProcessController_withoutAttachAPI_returnsFileProcessController()
      throws Exception {
    // arrange
    System.setProperty(PROPERTY_DISABLE_ATTACH_API, "true");
    factory = new ProcessControllerFactory();

    // act
    ProcessController controller = factory.createProcessController(parameters, pid);

    // assert
    assertThat(controller).isInstanceOf(FileProcessController.class);
  }

  @Test
  public void createProcessController_withNullParameters_throwsNullPointerException()
      throws Exception {
    // arrange
    parameters = null;

    // act/assert
    assertThatThrownBy(() -> factory.createProcessController(parameters, pid))
        .isInstanceOf(NullPointerException.class)
        .hasMessageContaining("Invalid parameters 'null' specified");
  }

  @Test
  public void createProcessController_withZeroPid_throwsIllegalArgumentException()
      throws Exception {
    // arrange
    pid = 0;

    // act/assert
    assertThatThrownBy(() -> factory.createProcessController(parameters, 0))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid pid '" + 0 + "' specified");
  }
}
