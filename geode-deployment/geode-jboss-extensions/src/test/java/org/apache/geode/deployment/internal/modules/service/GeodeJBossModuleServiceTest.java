/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.deployment.internal.modules.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.geode.deployment.internal.modules.loader.GeodeModuleLoader;

public class GeodeJBossModuleServiceTest {

  private static GeodeJBossModuleService geodeJBossModuleService;

  @BeforeClass
  public static void setup() {
    GeodeModuleLoader geodeModuleLoader = mock(GeodeModuleLoader.class);
    geodeJBossModuleService = new GeodeJBossModuleService(geodeModuleLoader);
  }

  @Test
  public void testLinkModule() {
    boolean moduleLinked = geodeJBossModuleService
        .linkModule("my-module", "default_application", true);

    assertThat(moduleLinked).isTrue();
  }

  @Test
  public void testLinkModuleNullModuleName() {
    assertThatThrownBy(() -> geodeJBossModuleService
        .linkModule(null, "default_application", true))
            .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testUnregisterModule() {
    boolean unregisterModule = geodeJBossModuleService.unregisterModule("my-module");
    assertThat(unregisterModule).isTrue();
  }

  @Test
  public void testUnregisterModuleWithNullModuleName() {
    assertThatThrownBy(() -> geodeJBossModuleService
        .unregisterModule(null))
            .hasMessageContaining("Module name cannot be null");
  }
}
