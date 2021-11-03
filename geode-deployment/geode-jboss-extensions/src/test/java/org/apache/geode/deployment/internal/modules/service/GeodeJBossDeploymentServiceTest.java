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

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.deployment.internal.modules.loader.GeodeModuleLoader;
import org.apache.geode.test.compiler.JarBuilder;

public class GeodeJBossDeploymentServiceTest {

  @ClassRule
  public static TemporaryFolder stagingTempDir = new TemporaryFolder();
  private static GeodeJBossDeploymentService geodeJBossDeploymentService;
  private static File myJar;

  @BeforeClass
  public static void setup() throws IOException {
    JarBuilder jarBuilder = new JarBuilder();
    GeodeModuleLoader geodeModuleLoader = mock(GeodeModuleLoader.class);
    geodeJBossDeploymentService = new GeodeJBossDeploymentService(geodeModuleLoader);
    myJar = new File(stagingTempDir.newFolder(), "myJar.jar");
    jarBuilder.buildJarFromClassNames(myJar, "Class1");
  }

  @Test
  public void testRegisterModule() {
    List<String> dependencies = Collections.singletonList("other-module");
    boolean moduleRegistered = geodeJBossDeploymentService
        .registerModule("my-module", myJar.getPath(), dependencies);

    assertThat(moduleRegistered).isTrue();
  }

  @Test
  public void testRegisterModuleNullDependencies() {
    boolean moduleRegistered = geodeJBossDeploymentService
        .registerModule("my-module", myJar.getPath(), null);

    assertThat(moduleRegistered).isTrue();
  }

  @Test
  public void testRegisterModuleNullModuleName() {
    assertThatThrownBy(() -> geodeJBossDeploymentService
        .registerModule(null, myJar.getPath(), null))
            .hasMessageContaining("Module name cannot be null");
  }

  @Test
  public void testRegisterModuleNullFilePath() {
    assertThatThrownBy(() -> geodeJBossDeploymentService
        .registerModule("my-module", null, null))
            .hasMessageContaining("File path cannot be null");
  }

  @Test
  public void testUnregisterModule() {
    boolean unregisterModule = geodeJBossDeploymentService.unregisterModule("my-module");
    assertThat(unregisterModule).isTrue();
  }

  @Test
  public void testUnregisterModuleWithNullModuleName() {
    assertThatThrownBy(() -> geodeJBossDeploymentService
        .unregisterModule(null))
            .hasMessageContaining("Module name cannot be null");
  }
}
